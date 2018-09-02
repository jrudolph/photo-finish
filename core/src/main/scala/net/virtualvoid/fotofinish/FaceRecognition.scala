package net.virtualvoid.fotofinish

import net.virtualvoid.facerecognition.Face
import net.virtualvoid.facerecognition.FaceRecognitionLib
import spray.json.JsonFormat

import scala.collection.immutable
import scala.reflect.ClassTag

object FaceRecognition {
  val MaxFaces = 100
  def detectFaces(imgFileName: String, numJitters: Int): FaceData = {
    val faces = (new Face.ByReference).toArray(MaxFaces).asInstanceOf[Array[Face.ByReference]]
    val numRes =
      FaceRecognitionLib.detect_faces(
        imgFileName,
        faces(0),
        faces.size,
        numJitters)

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

object FaceDataExtractor extends MetadataExtractor {
  val NumJitters = 1
  type EntryT = FaceData

  override def kind: String = "net.virtualvoid.fotofinish.FaceData"
  override def version: Int = 3

  override def classTag: ClassTag[FaceData] = scala.reflect.classTag[FaceData]

  override protected def extract(file: FileInfo): FaceData = FaceRecognition.detectFaces(file.repoFile.getAbsolutePath, NumJitters)
  override implicit val metadataFormat: JsonFormat[FaceData] = {
    import spray.json.DefaultJsonProtocol._
    implicit val rectangleFormat = jsonFormat4(Rectangle)
    implicit val faceFormat = jsonFormat2(FaceInfo)
    implicit val settingsFormat = jsonFormat2(RecognizerSettings)

    jsonFormat2(FaceData)
  }
}