package net.virtualvoid.fotofinish

import java.io.File
import java.io.FileInputStream
import java.security.MessageDigest

import akka.util.ByteString

import scala.annotation.tailrec
import scala.util.Try

sealed trait HashAlgorithm {
  def name: String
  def bitLength: Int
  def byteLength: Int
  def hexStringLength: Int
  def apply(file: File): Hash

  protected def algorithm: String
}
object HashAlgorithm {
  val Sha512: HashAlgorithm = new Impl("SHA-512")
  val Algorithms = Vector(Sha512)
  val StepSize = 65536

  def byName(name: String): Option[HashAlgorithm] =
    name match {
      case "sha-512" => Some(Sha512)
      case _         => Algorithms.find(_.name == name)
    }

  private class Impl(val algorithm: String) extends HashAlgorithm { hashAlgorithm =>
    require(!name.contains(":"))

    override def name: String = algorithm.toLowerCase

    private def createDigest(): MessageDigest =
      MessageDigest.getInstance(algorithm)

    val byteLength: Int = createDigest().getDigestLength
    val bitLength: Int = byteLength * 8
    val hexStringLength: Int = byteLength * 2 // one hex char per 4 bits

    def apply(file: File): Hash = {
      val digest = createDigest()
      val fis = new FileInputStream(file)
      val buffer = new Array[Byte](StepSize)

      @tailrec
      def hashStep(): ByteString =
        if (fis.available() > 0) {
          val read = fis.read(buffer)
          digest.update(buffer, 0, read)
          hashStep()
        } else ByteString(digest.digest())

      val hashData = try hashStep() finally fis.close()
      Hash(hashAlgorithm, hashData)
    }
  }
}
case class Hash(hashAlgorithm: HashAlgorithm, data: ByteString) {
  lazy val asHexString: String = data.map(_ formatted "%02x").mkString

  override def toString: String = s"${hashAlgorithm.name}:$asHexString"
}
object Hash {
  def fromPrefixedString(prefixed: String): Option[Hash] = Try {
    val Array(name, value) = prefixed.split(':')
    // TODO: fix error conditions
    val alg = HashAlgorithm.byName(name).get
    fromString(alg, value)
  }.toOption
  def fromString(hashAlgorithm: HashAlgorithm, string: String): Hash = {
    require(string.length == hashAlgorithm.hexStringLength)

    @tailrec
    def read(target: Array[Byte], at: Int): ByteString =
      if (at < target.length) {
        val twoHex = string.substring(at * 2, at * 2 + 2)
        target(at) = java.lang.Short.parseShort(twoHex, 16).toByte
        read(target, at + 1)
      } else
        ByteString(target)

    //val data = ByteString(string.grouped(2).map(s => java.lang.Short.parseShort(s, 16).toByte).toVector: _*)
    val data = read(new Array[Byte](hashAlgorithm.byteLength), at = 0)
    Hash(hashAlgorithm, data)
  }

  implicit val byteStringOrdering: Ordering[ByteString] = (x: ByteString, y: ByteString) => {
    val len = x.length min y.length

    @tailrec def step(idx: Int): Int =
      if (idx < len) {
        val s = x(idx).compareTo(y(idx))
        if (s == 0) step(idx + 1)
        else s
      } else x.length.compareTo(y.length)

    step(0)
  }
  implicit val hashOrdering: Ordering[Hash] = Ordering.by[Hash, ByteString](_.data)

  import spray.json._
  implicit val hashFormat = new JsonFormat[Hash] {
    override def read(json: JsValue): Hash = json match {
      case JsString(data) => Hash.fromPrefixedString(data).getOrElse(throw DeserializationException(s"Prefixed hash string could not be read [$data]"))
    }
    override def write(obj: Hash): JsValue = JsString(obj.toString)
  }
}