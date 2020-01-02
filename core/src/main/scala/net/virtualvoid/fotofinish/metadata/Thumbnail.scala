package net.virtualvoid.fotofinish.metadata

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream }

import akka.util.ByteString
import javax.imageio.ImageIO
import spray.json.{ DefaultJsonProtocol, JsonFormat }

import scala.concurrent.Future

final case class Thumbnail(
    width:  Int,
    height: Int,
    data:   ByteString
)
object Thumbnail extends MetadataKind.Impl[Thumbnail]("net.virtualvoid.fotofinish.metadata.Thumbnail", 1) {
  import DefaultJsonProtocol._
  import MetadataJsonProtocol.byteStringFormat
  implicit def jsonFormat: JsonFormat[Thumbnail] = jsonFormat3(Thumbnail.apply _)
}
object ThumbnailExtractor {
  val instance =
    MetadataExtractor("net.virtualvoid.fotofinish.metadata.ThumbnailExtractor", 1, Thumbnail) { (hash, ctx) =>
      ctx.accessData(hash) { file =>
        Future {
          import sys.process._
          val baos = new ByteArrayOutputStream
          val res = s"""convert ${file.getCanonicalPath} -thumbnail 150 -quality 20 -auto-orient -""".#>(baos).!
          require(res == 0, "convert didn't return successfully")
          val imageBytes = ByteString.fromArray(baos.toByteArray)
          // TODO: optimize memory usage
          val image = ImageIO.read(new ByteArrayInputStream(baos.toByteArray))

          Thumbnail(image.getWidth, image.getHeight, imageBytes)
        }(ctx.executionContext)
      }
    }
}
