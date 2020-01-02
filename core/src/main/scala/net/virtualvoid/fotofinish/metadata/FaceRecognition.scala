package net.virtualvoid.fotofinish
package metadata

import net.virtualvoid.facerecognition.{ Face, FaceRecognitionLib }
import spray.json.JsonFormat

import scala.collection.immutable

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
    FaceData(settings, infos.toVector)
  }
}

case class Rectangle(
    left:   Int,
    top:    Int,
    width:  Int,
    height: Int
)
case class FaceInfo(
    rectangle: Rectangle,
    modelData: Array[Float]
)
case class RecognizerSettings(
    recognizerLibVersion: Int,
    numJitters:           Int
)
case class FaceData(
    recognizerSettings: RecognizerSettings,
    faces:              immutable.Seq[FaceInfo]
)
object FaceData extends MetadataKind.Impl[FaceData]("net.virtualvoid.fotofinish.metadata.FaceData", 1) {
  implicit val jsonFormat: JsonFormat[FaceData] = {
    import spray.json.DefaultJsonProtocol._
    implicit val rectangleFormat = jsonFormat4(Rectangle)
    implicit val faceFormat = jsonFormat2(FaceInfo)
    implicit val settingsFormat = jsonFormat2(RecognizerSettings)

    jsonFormat2(FaceData.apply _)
  }
}

object FaceDataExtractor {
  val NumJitters = 1

  val instance =
    ImageDataExtractor.fromFileSync("net.virtualvoid.fotofinish.metadata.FaceDataExtractor", 1, FaceData) { file =>
      FaceRecognition.detectFaces(file.getAbsolutePath, NumJitters)
    }
}
