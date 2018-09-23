package net.virtualvoid.fotofinish.metadata

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

import akka.util.ByteString
import javax.imageio.ImageIO
import net.virtualvoid.fotofinish.FileInfo
import spray.json.DefaultJsonProtocol
import spray.json.JsonFormat

import scala.reflect.ClassTag

final case class Thumbnail(
    width:  Int,
    height: Int,
    data:   ByteString
)
object ThumbnailExtractor extends MetadataExtractor {
  type EntryT = Thumbnail

  override def kind: String = "thumbnail"
  override def version: Int = 2

  override def classTag: ClassTag[Thumbnail] = implicitly[ClassTag[Thumbnail]]

  override protected def extract(file: FileInfo): Thumbnail = {
    import sys.process._
    val baos = new ByteArrayOutputStream
    val res = s"""convert ${file.repoFile.getCanonicalPath} -thumbnail 150 -quality 20 -auto-orient -""".#>(baos).!
    require(res == 0, "convert didn't return successfully")
    val imageBytes = ByteString.fromArray(baos.toByteArray)
    // TODO: optimize memory usage
    val image = ImageIO.read(new ByteArrayInputStream(baos.toByteArray))

    Thumbnail(image.getWidth, image.getHeight, imageBytes)
  }

  import DefaultJsonProtocol._
  import MetadataJsonProtocol.byteStringFormat
  override implicit val metadataFormat: JsonFormat[Thumbnail] = jsonFormat3(Thumbnail)
}
