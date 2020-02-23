package net.virtualvoid.fotofinish.util

import java.awt.geom.AffineTransform
import java.awt.image.{ AffineTransformOp, BufferedImage }
import java.io.{ ByteArrayOutputStream, File }

import akka.util.ByteString
import javax.imageio.ImageIO
import net.virtualvoid.fotofinish.metadata.{ Orientation, Rectangle }

import scala.util.control.NonFatal

object ImageTools {
  type ImageTransformation = File => ByteString

  def crop(rectangle: Rectangle): ImageTransformation = transformJpeg { image =>
    // rectangles might reach outside of actual image so we need to clip at image edges
    // to avoid exception with getSubimage

    val croppedX = math.max(0, math.min(image.getWidth, rectangle.left))
    val croppedY = math.max(0, math.min(image.getHeight, rectangle.top))
    val croppedWidth = math.min(rectangle.width, image.getWidth - croppedX)
    val croppedHeight = math.min(rectangle.height, image.getHeight - croppedY)
    try image.getSubimage(croppedX, croppedY, croppedWidth, croppedHeight)
    catch {
      case NonFatal(ex) =>
        throw new IllegalArgumentException(s"Crop rectangle: $rectangle image width: ${image.getWidth} height: ${image.getHeight} croppedX: $croppedX croppedY: $croppedY croppedWidth: $croppedWidth croppedHeight: $croppedHeight", ex)
    }
  }
  def cropJpegTran(rectangle: Rectangle): ImageTransformation = {
    import rectangle._
    withCmd(fileName => s"jpegtran -crop ${width}x$height+$left+$top $fileName")
      .recoverWith(_ => crop(rectangle))
  }

  def correctOrientation(orientation: Orientation): ImageTransformation = transformJpeg { image =>
    val width = image.getWidth
    val height = image.getHeight

    val t = new AffineTransform()

    val (dwidth, dheight) =
      orientation match {
        case Orientation.Normal =>
          (width, height)
        case Orientation.Clockwise180 =>
          t.translate(width, height)
          t.rotate(math.Pi)
          (width, height)
        case Orientation.Clockwise270 =>
          t.translate(height, 0)
          t.rotate(math.Pi / 2)
          (height, width)
        case Orientation.Clockwise90 =>
          t.translate(0, width)
          t.rotate(3 * math.Pi / 2)
          (height, width)
      }
    val destImage = new BufferedImage(dwidth, dheight, image.getType)
    val op = new AffineTransformOp(t, AffineTransformOp.TYPE_BICUBIC)
    op.filter(image, destImage)
    destImage
  }

  def transformJpeg(f: BufferedImage => BufferedImage): File => ByteString = { fileName =>
    val image = ImageIO.read(fileName)

    val baos = new ByteArrayOutputStream()
    ImageIO.write(f(image), "jpeg", baos)

    ByteString(baos.toByteArray)
  }
  def withCmd(cmd: String => String): ImageTransformation = f => {
    import sys.process._
    val baos = new ByteArrayOutputStream()
    val theCmd = cmd(f.getAbsolutePath)
    val res = (theCmd #> baos).!
    if (res != 0) throw new RuntimeException(s"Executing [$theCmd] returned error code $res")
    else ByteString(baos.toByteArray)
  }

  implicit class WithRecovery(val t: ImageTransformation) extends AnyVal {
    def recoverWith(r: Throwable => ImageTransformation): ImageTransformation =
      file =>
        try t(file)
        catch {
          case NonFatal(ex) => r(ex)(file)
        }
  }
}