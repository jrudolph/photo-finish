package net.virtualvoid.fotofinish.metadata

import spray.json.JsonFormat

import java.awt.image.BufferedImage
import java.awt.{ Graphics2D, Image }
import java.io.File
import javax.imageio.ImageIO

case class PHashData(
    entries: Seq[(PHashParameters, Long)]
)

case class PHashParameters(
    dctSize:          Int,
    hashArea:         Int,
    dctType:          String,
    scalingType:      String,
    thresholdingType: String
)

object PHashData extends MetadataKind.Impl[PHashData]("net.virtualvoid.fotofinish.metadata.PHashData", 1) {
  import spray.json.DefaultJsonProtocol._
  override implicit val jsonFormat: JsonFormat[PHashData] = {
    implicit val parameterFormat = jsonFormat5(PHashParameters.apply _)
    jsonFormat1(PHashData.apply _)
  }
}

object PHashDataExtractor {
  val parameters = Seq(
    PHashParameters(
      32, 8, "jpegDCT", "SCALE_SMOOTH", "sign" // use getScaledInstance with SCALE_SMOOTH and then draw on grayscale buffer
    ),
    PHashParameters(
      32, 8, "jpegDCT", "direct drawImage", "sign" // directly draw on grayscale buffer
    )
  )

  val instance =
    ImageDataExtractor.fromFileSync("net.virtualvoid.fotofinish.metadata.PHashDataExtractor", 1, PHashData) { file =>
      val srcImg = loadImage(file)

      val res =
        parameters.map { params =>
          val scaled = scaleImage(srcImg, params)
          val dctd = dct(scaled, params)
          val h = hash(dctd, params)
          params -> h
        }
      PHashData(res)
    }

  private def loadImage(file: File): BufferedImage = ImageIO.read(file)
  private def scaleImage(img: BufferedImage, params: PHashParameters): BufferedImage = {
    val Size = params.dctSize
    val dst = new BufferedImage(Size, Size, BufferedImage.TYPE_BYTE_GRAY)
    val g = dst.getGraphics.asInstanceOf[Graphics2D]
    params.scalingType match {
      case "SCALE_SMOOTH" =>
        val scaled0 = img.getScaledInstance(Size, Size, Image.SCALE_SMOOTH)
        g.drawImage(scaled0, 0, 0, null)
      case "direct drawImage" =>
        val g = dst.getGraphics.asInstanceOf[Graphics2D]
        g.drawImage(img, 0, 0, Size, Size, null)
      case s => throw new IllegalArgumentException(s"Unknown scaleType '$s'")
    }
    dst
  }
  private def dct(img: BufferedImage, params: PHashParameters): Array[Double] = {
    import params.dctSize
    val data = new Array[Double](dctSize * dctSize)
    img.getRaster.getSamples(0, 0, dctSize, dctSize, 0, data)
    params.dctType match {
      case "jpegDCT" => jpegDCT2D(data)
      case s         => throw new IllegalArgumentException(s"Unknown dctType '$s'")
    }
  }
  private def hash(dct: Array[Double], params: PHashParameters): Long = {
    import params._
    def idx(x: Int, y: Int): Int = y * dctSize + x

    val thresholder: Double => Boolean = params.thresholdingType match {
      case "sign" => _ >= 0
      case s      => throw new IllegalArgumentException(s"Unknown thresholdingType '$s'")
    }

    val vals =
      for {
        y <- 0 until hashArea
        x <- 0 until hashArea
      } yield if (thresholder(dct(idx(x, y)))) "1" else "0"

    // FIXME: yeah, slow way to build a long from bits, but algorithm is anyway dominated by image scaling
    java.lang.Long.parseLong(
      vals.drop(1) /* drop DC value */
        .mkString,
      2
    )
  }

  /**
   * Naive JPEG-style DCT
   */
  private def jpegDCT2D(block: Array[Double]): Array[Double] = {

    val N = math.sqrt(block.length).toInt
    require(N * N == block.length, s"block must be square but size was ${block.length}")

    val factor0 = 1d / math.sqrt(2)
    def C(u: Int): Double = if (u == 0) factor0 else 1
    def idx(x: Int, y: Int): Int = y * N + x
    def s(x: Int, y: Int): Double = block(idx(x, y))

    {
      for {
        u <- 0 until N
        v <- 0 until N
      } yield {
        (2d / N) * C(u) * C(v) *
          (0 until N).map { x =>
            (0 until N).map { y =>
              s(x, y) * math.cos((2 * x + 1) * u * math.Pi / 2 / N) * math.cos((2 * y + 1) * v * math.Pi / 2 / N)
            }.sum
          }.sum
      }
    }.toArray
  }
}