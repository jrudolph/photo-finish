package net.virtualvoid.fotofinish
package metadata

import java.io.{ File, FileOutputStream }

import net.virtualvoid.facerecognition.{ Face, FaceRecognitionLib }
import net.virtualvoid.fotofinish.metadata.MetadataKind.Aux
import net.virtualvoid.fotofinish.util.ImageTools
import spray.json.JsonFormat
import spray.json.DefaultJsonProtocol._

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.control.NonFatal

object FaceRecognition {
  val MaxFaces = 100
  def detectFaces(imgFileName: String, numJitters: Int): FaceData = {
    val faces = (new Face.ByReference).toArray(MaxFaces).asInstanceOf[Array[Face.ByReference]]
    val numRes =
      FaceRecognitionLib.detect_faces(
        imgFileName,
        faces(0),
        faces.length,
        numJitters)

    if (numRes == -1)
      throw new RuntimeException("Face detection failed")

    val infos =
      faces.take(numRes).map {
        case f: Face.ByReference =>
          import f._
          val rect = Rectangle(left.toInt, top.toInt, right.toInt - left.toInt, bottom.toInt - top.toInt)
          FaceInfo(rect, model)
      }

    val settings = RecognizerSettings(1, numJitters)
    FaceData(settings, None, infos.toVector)
  }
}

case class Rectangle(
    left:   Int,
    top:    Int,
    width:  Int,
    height: Int
)
object Rectangle {
  implicit val rectangleFormat = jsonFormat4(Rectangle.apply _)
}
case class FaceInfo(
    rectangle: Rectangle, // bounding rectangle on original image, i.e. not taking orientation into account
    modelData: Array[Float]
)
object FaceInfo {
  implicit val faceFormat = jsonFormat2(FaceInfo.apply _)
}
case class RecognizerSettings(
    recognizerLibVersion: Int,
    numJitters:           Int
)
case class FaceData(
    recognizerSettings: RecognizerSettings,
    orientation:        Option[Orientation], // orientation should be same as ExifBaseData.orientation, replicated here to ease rotating image while loading
    faces:              immutable.Seq[FaceInfo]
)
object FaceData extends MetadataKind.Impl[FaceData]("net.virtualvoid.fotofinish.metadata.FaceData", 1) {
  implicit val jsonFormat: JsonFormat[FaceData] = {
    implicit val settingsFormat = jsonFormat2(RecognizerSettings)

    jsonFormat3(FaceData.apply _)
  }
}

object FaceDataExtractor {
  val NumJitters = 1

  val instance =
    new MetadataExtractor {
      type EntryT = FaceData
      def kind: String = "net.virtualvoid.fotofinish.metadata.FaceDataExtractor"
      def version: Int = 3
      def metadataKind: Aux[FaceData] = FaceData
      def dependsOn: Vector[MetadataKind] = Vector(FileTypeData, ExifBaseData)

      def extractEntry(hash: Hash, dependencies: Vector[MetadataEntry], ctx: ExtractionContext): Future[FaceData] =
        ctx.accessDataSync(hash) { file =>
          val tmpFile = File.createTempFile("rotated", "jpeg")
          try {
            val exifBaseData = dependencies(1).value.asInstanceOf[ExifBaseData]
            val imageWidth = exifBaseData.width.get
            val imageHeight = exifBaseData.height.get
            val orientation = exifBaseData.orientation
            val target =
              orientation match {
                case Some(Orientation.Normal) | None => file
                case Some(o) =>
                  val rotated = ImageTools.correctOrientationJpegTran(o)(file)
                  val fos = new FileOutputStream(tmpFile)
                  fos.write(rotated.toArray)
                  fos.close()
                  tmpFile
              }

            def postProcessInfo(faceInfo: FaceInfo): FaceInfo =
              orientation match {
                case Some(Orientation.Normal) | None => faceInfo
                case Some(o) =>
                  val rect = faceInfo.rectangle
                  val newRectangle = o match {
                    case Orientation.Clockwise90 =>
                      // 0,0 => width,0
                      // height,0 => width,height
                      // 0,width => 0,0
                      // height, width => 0,height
                      Rectangle(imageWidth - rect.top - rect.height, rect.left, rect.height, rect.width)

                    case Orientation.Clockwise180 =>
                      Rectangle(imageWidth - rect.left - rect.width, imageHeight - rect.top - rect.height, rect.width, rect.height)
                    case Orientation.Clockwise270 =>
                      // 0,0 => 0,height
                      // height,0 => 0, 0
                      // 0,width => width,height
                      // height, width => width,0
                      Rectangle(rect.top, imageHeight - rect.left - rect.width, rect.height, rect.width)
                  }

                  faceInfo.copy(rectangle = newRectangle)
              }

            val res = FaceRecognition.detectFaces(target.getAbsolutePath, NumJitters)
            res.copy(orientation = orientation, faces = res.faces.map(postProcessInfo))

          } catch {
            case NonFatal(ex) =>
              throw new RuntimeException(s"FaceRecognition failed for hash [${hash.asHexString}] because of ${ex.getClass} ${ex.getMessage}", ex)
          } finally tmpFile.delete()
        }
      override def precondition(hash: Hash, dependencies: Vector[MetadataEntry]): Option[String] = {
        val mime = dependencies(0).value.asInstanceOf[FileTypeData].mimeType
        val exif = dependencies(1).value.asInstanceOf[ExifBaseData]
        if (mime.startsWith("image/"))
          if (exif.orientation.exists(_ != Orientation.Normal))
            if (exif.width.isDefined && exif.height.isDefined) None
            else Some("Cannot fix orientation without width and height")
          else None
        else Some(s"Object is not an image but [$mime]")
      }

      override def upgradeExisting(existing: MetadataEntry.Aux[FaceData], dependencies: Vector[MetadataEntry]): MetadataExtractor.Upgrade = {
        val orientation = dependencies(1).value.asInstanceOf[ExifBaseData].orientation
        val needsRerun = (orientation, existing.value.orientation) match {
          case (Some(Orientation.Normal), None) => false // fine, we didn't note any orientation but also we won't do better than before
          case (x, y)                           => x != y
        }
        if (needsRerun) MetadataExtractor.RerunExtractor
        else MetadataExtractor.Keep
      }
    }

  /*ImageDataExtractor.fromFileSync("net.virtualvoid.fotofinish.metadata.FaceDataExtractor", 1, FaceData) { file =>
    FaceRecognition.detectFaces(file.getAbsolutePath, NumJitters)
  }*/
}
