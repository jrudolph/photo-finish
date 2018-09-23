package net.virtualvoid.fotofinish.util

import java.awt.geom.AffineTransform
import java.awt.image.AffineTransformOp
import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import java.io.File

import akka.util.ByteString
import javax.imageio.ImageIO
import net.virtualvoid.fotofinish.metadata.Orientation
import net.virtualvoid.fotofinish.metadata.Rectangle

object ImageTools {
  type ImageTransformation = File => ByteString

  def crop(rectangle: Rectangle): ImageTransformation = transformJpeg { image =>
    // rectangles might reach outside of actual image so we need to clip at image edges
    // to avoid exception with getSubimage
    val croppedWidth = math.min(rectangle.width, image.getWidth - rectangle.left)
    val croppedHeight = math.min(rectangle.height, image.getHeight - rectangle.top)
    image.getSubimage(rectangle.left, rectangle.top, croppedWidth, croppedHeight)
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
}