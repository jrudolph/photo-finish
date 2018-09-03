package net.virtualvoid.fotofinish

import java.io.File
import java.io.FileFilter
import java.io.FileInputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFileAttributes
import java.security.MessageDigest

import akka.util.ByteString

import scala.annotation.tailrec
import scala.collection.immutable
import scala.util.Try

import metadata._

object Settings {
  val repo = new File("/home/johannes/Fotos/tmp/repo")

  val repoConfig = RepositoryConfig(repo, HashAlgorithm.Sha512)
  val manager = new RepositoryManager(repoConfig)
}
object MainScanner extends App {

  import Settings._

  val dirs =
    {
      (2003 to 2020).map(year => s"/home/johannes/Fotos/$year") ++
        Seq[String]()
    }
      .map(new File(_))
      .filter(_.exists)

  def ingestDir(dir: File): immutable.Seq[FileInfo] = {
    println(s"Ingesting new files from $dir")
    val infos = new Scanner(repoConfig, manager).scan(dir)

    println("Updating basic metadata for ingested fotos")
    infos.par.foreach(MetadataStore.updateMetadataFor(_, IngestionDataExtractor, repoConfig))

    println("Updating exif metadata for ingested fotos")
    infos.par.foreach(MetadataStore.updateMetadataFor(_, ExifBaseDataExtractor, repoConfig))

    println("Updating all metadata for ingested fotos")
    infos.par.foreach(MetadataStore.updateMetadata(_, repoConfig))

    infos
  }
  dirs.flatMap(ingestDir)

  println("Updating remaining metadata")
  manager.allRepoFiles().toStream.par
    .foreach(MetadataStore.updateMetadata(_, repoConfig))

  println("Updating by-date folder")
  Relinker.createDirStructure(manager)(Relinker.byYearMonth(manager))

  println("Updating by-original-name folder")
  Relinker.createDirStructure(manager)(Relinker.byOriginalFileName(manager))
}

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

  def fileInfoByHashPrefix(prefix: String, hashAlgorithm: HashAlgorithm = hashAlgorithm): Option[FileInfo] = {
    require(prefix.size > 2)

    val dir = new File(storageDir, s"by-${hashAlgorithm.name}/${prefix.take(2)}/")
    import Scanner._
    dir.listFiles(byFileName(name => name.startsWith(prefix) && name.length == hashAlgorithm.hexStringLength))
      .headOption
      .map(f => fileInfoOf(Hash.fromString(hashAlgorithm, f.getName)))
  }

}

class Scanner(config: RepositoryConfig, manager: RepositoryManager) {
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
    val ufi = unixFileInfo(file.toPath)
    val byInode = manager.inodeMap.get((ufi.dev, ufi.inode))
    if (byInode.isDefined) {
      println(s"Found hard link into repo for [$file] at [${byInode.get}]")
      byInode.get.copy(originalFile = file)
    } else {
      val hash = Hasher.hash(hashAlgorithm, file)
      val inRepo = repoFile(hash)
      if (!inRepo.exists()) {
        println(s"Creating repo file for [$file] at [$inRepo] exists: ${inRepo.exists()}")
        Files.createDirectories(inRepo.getParentFile.toPath)
        if (Files.getFileStore(file.toPath.toRealPath()) == Files.getFileStore(inRepo.toPath.getParent.toRealPath()))
          Files.createLink(inRepo.toPath, file.toPath)
        else
          Files.copy(file.toPath, inRepo.toPath)

        inRepo.setWritable(false)
      } else {
        println(s"Already in repo [$file] (as determined by hash)")
        // TODO: create hard-link instead?
      }

      FileInfo(hash, inRepo, metadataFile(hash), file)
    }
  }
}

object Scanner {
  val supportedFiles = withExtensions("jpg", "jpeg")

  def allFilesMatching(dir: File, fileFilter: FileFilter): Iterable[File] = {
    def iterate(dir: File): Iterator[File] = {
      val subDirs = Option(dir.listFiles(isDirectory && isNoDotDir)).getOrElse(Array.empty)

      Option(dir.listFiles(fileFilter)).getOrElse(Array.empty).toIterator ++
        subDirs.toIterator.flatMap(dir => iterate(dir))
    }

    new Iterable[File] {
      override def iterator: Iterator[File] = iterate(dir)
    }
  }

  case class UnixFileInfo(dev: Long, inode: Long)

  private val (inoF, devF) = {
    val clazz = Class.forName("sun.nio.fs.UnixFileAttributes")
    val inoF = clazz.getDeclaredMethod("ino")
    inoF.setAccessible(true)
    val devF = clazz.getDeclaredMethod("dev")
    devF.setAccessible(true)
    (inoF, devF)
  }
  def unixFileInfo(path: Path): UnixFileInfo = {
    val posix = Files.readAttributes(path, classOf[PosixFileAttributes])
    val (ino: Long, dev: Long) = (inoF.invoke(posix): Any, devF.invoke(posix): Any)
    UnixFileInfo(dev, ino)
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
  import Scanner._

  def metadataFor(hash: Hash): Metadata =
    MetadataStore.load(config.fileInfoOf(hash))

  def allRepoFiles(): Iterator[FileInfo] =
    Scanner.allFilesMatching(config.primaryStorageDir, byFileName(str => FileNamePattern.findFirstMatchIn(str).isDefined))
      .iterator
      .map(f => Hash.fromString(config.hashAlgorithm, f.getName))
      .map(config.fileInfoOf)

  def allFiles(): Iterator[FileAndMetadata] =
    allRepoFiles()
      .map { fileInfo =>
        FileAndMetadata(fileInfo, MetadataStore.load(fileInfo))
      }

  lazy val inodeMap: Map[(Long, Long), FileInfo] =
    allRepoFiles()
      .map { info =>
        val UnixFileInfo(dev, ino) = Scanner.unixFileInfo(info.repoFile.toPath)

        (dev, ino) -> info
      }.toMap
}