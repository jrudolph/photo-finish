package net.virtualvoid.fotofinish.metadata

import akka.http.scaladsl.model.DateTime
import com.drew.imaging.ImageMetadataReader
import com.drew.metadata.Directory
import com.drew.metadata.exif.{ ExifDirectoryBase, ExifIFD0Directory, ExifSubIFDDirectory }
import spray.json.{ DefaultJsonProtocol, JsString, JsValue, JsonFormat }

import scala.util.Try

sealed trait Orientation
object Orientation {
  final case object Normal extends Orientation
  final case object Clockwise90 extends Orientation
  final case object Clockwise180 extends Orientation
  final case object Clockwise270 extends Orientation

  val values = Seq(Normal, Clockwise90, Clockwise180, Clockwise270)

  implicit val format = new JsonFormat[Orientation] {
    override def write(obj: Orientation): JsValue = JsString(obj.toString)
    override def read(json: JsValue): Orientation = json match {
      case JsString(value) => values.find(_.toString == value).get
      case x               => MetadataJsonProtocol.error(s"Cannot read Orientation from $x")
    }
  }
}

final case class ExifBaseData(
    width:       Option[Int],
    height:      Option[Int],
    dateTaken:   Option[DateTime],
    cameraModel: Option[String],
    orientation: Option[Orientation]
)
object ExifBaseData extends MetadataKind.Impl[ExifBaseData](
  "net.virtualvoid.fotofinish.metadata.ExifBaseData",
  1
) {
  import DefaultJsonProtocol._
  import MetadataJsonProtocol.dateTimeFormat
  implicit def jsonFormat: JsonFormat[ExifBaseData] =
    jsonFormat5(ExifBaseData.apply _)
}

object ExifBaseDataExtractor {
  val instance =
    MetadataExtractor("net.virtualvoid.fotofinish.metadata.ExifBaseDataExtractor", 1, ExifBaseData) { (hash, ctx) =>
      ctx.accessDataSync(hash) { data =>
        val metadata = ImageMetadataReader.readMetadata(data)
        val dir0 = Option(metadata.getFirstDirectoryOfType(classOf[ExifIFD0Directory]))
        val dir1 = Option(metadata.getFirstDirectoryOfType(classOf[ExifSubIFDDirectory]))
        val dirs = dir0.toSeq ++ dir1.toSeq

        def entry[T](tped: (Directory, Int) => T): Int => Option[T] =
          tag =>
            dirs.collectFirst {
              case dir if dir.containsTag(tag) => Option(tped(dir, tag))
            }.flatten

        def dateEntry = entry((dir, tag) => Try(DateTime(dir.getDate(tag).getTime)).toOption.orNull) // may fail if outside reasonable date range
        def intEntry = entry(_.getInt(_))
        def stringEntry = entry(_.getString(_))

        val width = intEntry(ExifDirectoryBase.TAG_EXIF_IMAGE_WIDTH)
        val height = intEntry(ExifDirectoryBase.TAG_EXIF_IMAGE_HEIGHT)
        val date = dateEntry(ExifDirectoryBase.TAG_DATETIME_ORIGINAL)
        val model = stringEntry(ExifDirectoryBase.TAG_MODEL)
        val orientation = intEntry(ExifDirectoryBase.TAG_ORIENTATION).map {
          case 0 | 1 => Orientation.Normal
          case 3     => Orientation.Clockwise180
          case 6     => Orientation.Clockwise270
          case 8     => Orientation.Clockwise90
        }

        ExifBaseData(width, height, date, model, orientation)
      }
    }
}
