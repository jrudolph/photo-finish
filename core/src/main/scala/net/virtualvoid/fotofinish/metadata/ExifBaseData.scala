package net.virtualvoid.fotofinish.metadata

import akka.http.scaladsl.model.DateTime
import com.drew.imaging.ImageMetadataReader
import com.drew.metadata.Directory
import com.drew.metadata.exif.ExifDirectoryBase
import com.drew.metadata.exif.ExifIFD0Directory
import com.drew.metadata.exif.ExifSubIFDDirectory
import net.virtualvoid.fotofinish.FileInfo
import spray.json.DefaultJsonProtocol
import spray.json.JsonFormat

import scala.reflect.ClassTag

final case class ExifBaseData(
    width:       Option[Int],
    height:      Option[Int],
    dateTaken:   Option[DateTime],
    cameraModel: Option[String]
)
object ExifBaseDataExtractor extends MetadataExtractor {
  override type EntryT = ExifBaseData

  override def kind: String = "exif-base-data"
  override def version: Int = 1

  override def classTag: ClassTag[ExifBaseData] = implicitly[ClassTag[ExifBaseData]]

  import DefaultJsonProtocol._
  import MetadataJsonProtocol.dateTimeFormat
  override implicit def metadataFormat: JsonFormat[ExifBaseData] = jsonFormat4(ExifBaseData)

  override protected def extract(file: FileInfo): ExifBaseData = {
    val metadata = ImageMetadataReader.readMetadata(file.repoFile)
    val dir0 = Option(metadata.getFirstDirectoryOfType(classOf[ExifIFD0Directory]))
    val dir1 = Option(metadata.getFirstDirectoryOfType(classOf[ExifSubIFDDirectory]))
    val dirs = dir0.toSeq ++ dir1.toSeq

    def entry[T](tped: (Directory, Int) => T): Int => Option[T] =
      tag =>
        dirs.collectFirst {
          case dir if dir.containsTag(tag) => Option(tped(dir, tag))
        }.flatten
    def dateEntry = entry((dir, tag) => DateTime(dir.getDate(tag).getTime))
    def intEntry = entry(_.getInt(_))
    def stringEntry = entry(_.getString(_))

    val width = intEntry(ExifDirectoryBase.TAG_EXIF_IMAGE_WIDTH)
    val height = intEntry(ExifDirectoryBase.TAG_EXIF_IMAGE_HEIGHT)
    val date = dateEntry(ExifDirectoryBase.TAG_DATETIME_ORIGINAL)
    val model = stringEntry(ExifDirectoryBase.TAG_MODEL)

    ExifBaseData(width, height, date, model)
  }
}