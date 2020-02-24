package net.virtualvoid.fotofinish.util

import java.awt.geom.AffineTransform
import java.awt.image.{ AffineTransformOp, BufferedImage }
import java.io.{ ByteArrayOutputStream, File, FileOutputStream }

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

  trait JpegTranTransformation {
    def withOrientationCorrection(orientation: Orientation): JpegTranTransformation
    def withOrientationCorrection(orientation: Option[Orientation]): JpegTranTransformation
    def withCrop(rectangle: Rectangle): JpegTranTransformation
    def build(): ImageTransformation
  }
  object JpegTranTransformation {
    def apply(): JpegTranTransformation = Impl(None, None)

    private case class Impl(
        crop:               Option[Rectangle],
        correctOrientation: Option[Orientation]) extends JpegTranTransformation {
      override def withOrientationCorrection(orientation: Orientation): JpegTranTransformation =
        copy(correctOrientation = Some(orientation))
      override def withOrientationCorrection(orientation: Option[Orientation]): JpegTranTransformation =
        copy(correctOrientation = orientation)

      override def withCrop(rectangle: Rectangle): JpegTranTransformation =
        copy(crop = Some(rectangle))
      override def build(): ImageTransformation = {
        val c = crop.fold("") { rect =>
          import rect._
          s"-crop ${width}x$height+$left+$top"
        }
        val correctFactor = correctOrientation.fold(Option.empty[Int]) {
          case Orientation.Normal       => None
          case Orientation.Clockwise90  => Some(270)
          case Orientation.Clockwise180 => Some(180)
          case Orientation.Clockwise270 => Some(90)
        }
        val rotate = correctFactor.fold("")(i => s"-rotate $i")
        withCmd(fileName => s"jpegtran $c $rotate $fileName")
      }
    }
  }

  def cropJpegTran(rectangle: Rectangle): ImageTransformation =
    JpegTranTransformation().withCrop(rectangle).build()
      .recoverWith(_ => crop(rectangle))

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
  def correctOrientationJpegTran(orientation: Orientation): ImageTransformation =
    JpegTranTransformation().withOrientationCorrection(orientation).build()

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
    def and(next: ImageTransformation): ImageTransformation = { file =>
      val res = t(file)
      val tmpFile = File.createTempFile("trans", "tmp")
      try {
        val fos = new FileOutputStream(tmpFile)
        fos.write(res.toArray)
        fos.close()
        next(tmpFile)
      } finally tmpFile.delete()
    }
  }
}