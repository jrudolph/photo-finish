package net.virtualvoid.fotofinish.util

import java.io.ByteArrayOutputStream
import java.io.File

import akka.util.ByteString
import javax.imageio.ImageIO
import net.virtualvoid.fotofinish.metadata.Rectangle

object ImageTools {
  def crop(fileName: File, rectangle: Rectangle): ByteString = {
    val image = ImageIO.read(fileName)

    // rectangles might reach outside of actual image so we need to clip at image edges
    // to avoid exception with getSubimage
    val croppedWidth = math.min(rectangle.width, image.getWidth - rectangle.left)
    val croppedHeight = math.min(rectangle.height, image.getHeight - rectangle.top)
    val sub = image.getSubimage(rectangle.left, rectangle.top, croppedWidth, croppedHeight)

    val baos = new ByteArrayOutputStream()
    ImageIO.write(sub, "jpeg", baos)

    ByteString(baos.toByteArray)
  }
}