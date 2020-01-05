package net.virtualvoid.fotofinish

import java.io.File
import java.io.FileFilter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.PosixFileAttributes

import net.virtualvoid.fotofinish.metadata.IngestionData

import scala.Console._

class Scanner(config: RepositoryConfig) {
  import Scanner._
  import config._

  def scan(target: File): Iterator[(Hash, IngestionData)] = {
    val allFiles = allFilesMatching(target, supportedFiles)

    val numFiles = allFiles.size
    val totalSize = allFiles.map(_.length()).sum

    println(s"Found $numFiles files with total size $totalSize")

    allFiles.iterator.map(f => ensureInRepo(f))
  }

  def ensureInRepo(file: File): (Hash, IngestionData) = {
    val ufi = unixFileInfo(file.toPath)
    val byInode = inodeMap.get((ufi.dev, ufi.inode))
    if (byInode.isDefined) {
      // FIXME: we could also decide not to do anything if we find an existing link into the repo?
      // println(s"Found hard link into repo for [$file] at [${byInode.get}]")
      val fi = byInode.get
      fi.hash -> IngestionData.fromFileInfo(fi.copy(originalFile = Some(file)))
    } else {
      val hash = hashAlgorithm(file)
      val inRepo = repoFile(hash)
      // make sure to get original data before moving / linking files around
      val res = hash -> IngestionData.fromFileInfo(FileInfo(hash, inRepo, Some(file)))

      if (!inRepo.exists()) {
        println(s"${GREEN}Creating repo file$RESET for [$file] at [$inRepo] exists: ${inRepo.exists()}")
        Files.createDirectories(inRepo.getParentFile.toPath)
        if (Files.getFileStore(file.toPath.toRealPath()) == Files.getFileStore(inRepo.toPath.getParent.toRealPath()))
          Files.createLink(inRepo.toPath, file.toPath)
        else
          Files.copy(file.toPath, inRepo.toPath)

        inRepo.setWritable(false)
      } else {
        if (Files.getFileStore(file.toPath.toRealPath()) == Files.getFileStore(inRepo.toPath.getParent.toRealPath())) {
          println(s"Already in repo [$file] (as determined by hash), ${MAGENTA}replacing with link$RESET")
          val tmpPath = Paths.get(file.getAbsolutePath + ".tmp")
          Files.createLink(tmpPath, inRepo.toPath)
          Files.move(tmpPath, file.toPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        } else
          println(s"Already in repo [$file] (as determined by hash), ${RED}cannot replace with link because on different file system$RESET")
      }

      res
    }
  }

  // FIXME: could this be replaced by a process instead of running it every time?
  lazy val inodeMap: Map[(Long, Long), FileInfo] =
    scanAllRepoFiles()
      .map { info =>
        val UnixFileInfo(dev, ino) = Scanner.unixFileInfo(info.repoFile.toPath)

        (dev, ino) -> info
      }.toMap

  private val FileNamePattern = """^[0-9a-f]{128}$""".r
  private def scanAllRepoFiles(): Iterator[FileInfo] =
    Scanner.allFilesMatching(config.primaryStorageDir, byFileName(str => FileNamePattern.findFirstMatchIn(str).isDefined))
      .iterator
      .map(f => Hash.fromString(config.hashAlgorithm, f.getName))
      .map(config.fileInfoOf)
}

object Scanner {
  val supportedFiles = withExtensions("jpg", "jpeg", "txt")

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

