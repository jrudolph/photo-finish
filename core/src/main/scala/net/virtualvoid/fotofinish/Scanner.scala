package net.virtualvoid.fotofinish

import java.io.File
import java.io.FileFilter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFileAttributes

import scala.collection.immutable

class Scanner(config: RepositoryConfig, manager: RepositoryManager) {
  import Scanner._
  import config._

  def scan(target: File): immutable.Seq[FileInfo] = {
    val allFiles = allFilesMatching(target, supportedFiles)

    val numFiles = allFiles.size
    val totalSize = allFiles.map(_.length()).sum

    println(s"Found $numFiles files with total size $totalSize")

    allFiles.map(ensureInRepo).toVector
  }

  def ensureInRepo(file: File): FileInfo = {
    val ufi = unixFileInfo(file.toPath)
    val byInode = manager.inodeMap.get((ufi.dev, ufi.inode))
    if (byInode.isDefined) {
      println(s"Found hard link into repo for [$file] at [${byInode.get}]")
      byInode.get.copy(originalFile = Some(file))
    } else {
      val hash = hashAlgorithm(file)
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

      FileInfo(hash, inRepo, metadataFile(hash), Some(file))
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

