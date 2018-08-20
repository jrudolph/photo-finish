package net.virtualvoid.fotofinish

import java.io.File
import java.io.FileFilter
import java.io.FileInputStream
import java.nio.file.Files
import java.security.MessageDigest

import akka.util.ByteString

import scala.annotation.tailrec
import scala.collection.immutable
import scala.util.Try

object Main extends App {
  val dir = new File("/home/johannes/Fotos/tmp/data")
  val repo = new File("/home/johannes/Fotos/tmp/repo")

  val repoConfig = RepositoryConfig(repo, HashAlgorithm.Sha512)
  /*val infos = new Scanner(repoConfig).scan(dir)
  infos.foreach(MetadataStore.updateMetadata(_, repoConfig))*/

  val manager = new RepositoryManager(repoConfig)
  Relinker.createLinkedDirByYearMonth(manager)
}

sealed trait HashAlgorithm {
  def name: String
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
    // TODO: check length
    val data = ByteString(string.grouped(2).map(s => java.lang.Short.parseShort(s, 16).toByte).toVector: _*)
    Hash(hashAlgorithm, data)
  }
}

case class FileInfo(
    hash:         Hash,
    repoFile:     File,
    metadataFile: File,
    originalFile: File
)

case class RepositoryConfig(
    storageDir:    File,
    hashAlgorithm: HashAlgorithm
) {
  def primaryStorageDir: File = new File(storageDir, s"by-${hashAlgorithm.name}")

  def repoFile(hash: Hash): File = {
    val fileName = s"by-${hash.hashAlgorithm.name}/${hash.asHexString.take(2)}/${hash.asHexString}"
    new File(storageDir, fileName)
  }
  def metadataFile(hash: Hash): File = {
    val fileName = s"by-${hash.hashAlgorithm.name}/${hash.asHexString.take(2)}/${hash.asHexString}.metadata.json.gz"
    new File(storageDir, fileName)
  }

  def fileInfoOf(hash: Hash): FileInfo =
    FileInfo(
      hash,
      repoFile(hash),
      metadataFile(hash),
      repoFile(hash)
    )
}

class Scanner(config: RepositoryConfig) {
  import Scanner._
  import config._

  def scan(target: File): immutable.Seq[FileInfo] = {
    val allFiles = allFilesMatching(target, supportedFiles)

    val numFiles = allFiles.size
    val totalSize = allFiles.map(_.length()).sum

    println(s"Found $numFiles files with total size $totalSize")

    allFiles.foreach(println)

    allFiles.map(ensureInRepo).toVector
  }

  def ensureInRepo(file: File): FileInfo = {
    val hash = Hasher.hash(hashAlgorithm, file)
    val inRepo = repoFile(hash)
    if (!inRepo.exists()) {
      println(s"Creating repo file for [$file] at [$inRepo] exists: ${inRepo.exists()}")
      Files.createDirectories(inRepo.getParentFile.toPath)
      if (file.toPath.getFileSystem == inRepo.toPath.getFileSystem)
        Files.createLink(inRepo.toPath, file.toPath)
      else
        Files.copy(file.toPath, inRepo.toPath)

      inRepo.setWritable(false)
    }

    FileInfo(hash, inRepo, metadataFile(hash), file)
  }
}

object Scanner {
  val supportedFiles = withExtensions("jpg", "jpeg")

  def allFilesMatching(dir: File, fileFilter: FileFilter): Iterable[File] = {
    def iterate(dir: File): Iterator[File] = {
      val subDirs = dir.listFiles(isDirectory && isNoDotDir)

      dir.listFiles(fileFilter).toIterator ++
        subDirs.toIterator.flatMap(dir => iterate(dir))
    }

    new Iterable[File] {
      override def iterator: Iterator[File] = iterate(dir)
    }
  }

  import scala.language.implicitConversions
  implicit def predicateAsFileFilter(f: File => Boolean): FileFilter = f(_)
  def byFileName(pred: String => Boolean): FileFilter = f => pred(f.getName)
  def withExtension(ext: String): FileFilter = byFileName(_.toLowerCase.endsWith("." + ext.toLowerCase))
  def withExtensions(exts: String*): FileFilter = byFileName(name => exts.exists(ext => name.toLowerCase.endsWith("." + ext.toLowerCase)))
  val isDirectory: FileFilter = _.isDirectory
  val isNoDotDir = byFileName((name: String) => name != ".." && name != ".")

  implicit class RichFileFilter(val filter: FileFilter) extends AnyVal {
    def &&(other: FileFilter): FileFilter = file => filter.accept(file) && other.accept(file)
    def ||(other: FileFilter): FileFilter = file => filter.accept(file) || other.accept(file)
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

final case class FileAndMetadata(fileInfo: FileInfo, metadata: Metadata)
class RepositoryManager(val config: RepositoryConfig) {
  val FileNamePattern = """^[0-9a-f]{128}$""".r
  def allFiles: Iterable[FileAndMetadata] = {
    import Scanner._
    Scanner.allFilesMatching(config.storageDir, byFileName(str => FileNamePattern.findFirstMatchIn(str).isDefined))
      .map(f => Hash.fromString(config.hashAlgorithm, f.getName))
      .map(config.fileInfoOf)
      .map { fileInfo =>
        FileAndMetadata(fileInfo, MetadataStore.load(fileInfo))
      }
  }
}