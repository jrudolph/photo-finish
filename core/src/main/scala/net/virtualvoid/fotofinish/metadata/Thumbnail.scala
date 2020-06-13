package net.virtualvoid.fotofinish.metadata

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream }

import akka.util.ByteString
import javax.imageio.ImageIO
import spray.json.{ DefaultJsonProtocol, JsonFormat }

@deprecated("Thumbnail is not used any more", since = "0.1.0")
final case class Thumbnail(
    width:  Int,
    height: Int,
    data:   ByteString
)
// @deprecated("Thumbnail is not used any more", since = "0.1.0")
object Thumbnail extends MetadataKind.Impl[Thumbnail]("net.virtualvoid.fotofinish.metadata.Thumbnail", 1) {
  import DefaultJsonProtocol._
  import MetadataJsonProtocol.byteStringFormat
  implicit def jsonFormat: JsonFormat[Thumbnail] = jsonFormat3(Thumbnail.apply _)
}
@deprecated("Thumbnail is not used any more", since = "0.1.0")
object ThumbnailExtractor {
  val instance =
    ImageDataExtractor.fromFileSync("net.virtualvoid.fotofinish.metadata.ThumbnailExtractor", 1, Thumbnail) { file =>
      import sys.process._
      val baos = new ByteArrayOutputStream
      val res = s"""convert ${file.getCanonicalPath} -thumbnail 150 -quality 20 -auto-orient -""".#>(baos).!
      require(res == 0, "convert didn't return successfully")
      val imageBytes = ByteString.fromArray(baos.toByteArray)
      // TODO: optimize memory usage
      val image = ImageIO.read(new ByteArrayInputStream(baos.toByteArray))

      Thumbnail(image.getWidth, image.getHeight, imageBytes)
    }
}
