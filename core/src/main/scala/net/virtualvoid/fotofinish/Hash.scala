package net.virtualvoid.fotofinish

import java.io.File
import java.io.FileInputStream
import java.security.MessageDigest

import akka.util.ByteString

import scala.annotation.tailrec
import scala.util.Try

sealed trait HashAlgorithm {
  def name: String
  def bits: Int
  def hexStringLength: Int
  def createDigest(): MessageDigest
  protected def algorithm: String
}
object HashAlgorithm {
  val Sha512: HashAlgorithm = new Impl("SHA-512")
  val Algorithms = Vector(Sha512)

  def byName(name: String): Option[HashAlgorithm] = Algorithms.find(_.name == name)

  private class Impl(val algorithm: String) extends HashAlgorithm {
    require(!name.contains(":"))

    override def name: String = algorithm.toLowerCase

    override def createDigest(): MessageDigest =
      MessageDigest.getInstance(algorithm)

    val bits: Int = createDigest().getDigestLength * 8
    val hexStringLength: Int = bits / 4 // one hex char per 4 bits
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
    val data = ByteString(string.grouped(2).map(s => java.lang.Short.parseShort(s, 16).toByte).toVector: _*)
    Hash(hashAlgorithm, data)
  }
}

object Hasher {
  val StepSize = 65536

  def hash(hashAlgorithm: HashAlgorithm, file: File): Hash = {
    val digest = hashAlgorithm.createDigest()
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