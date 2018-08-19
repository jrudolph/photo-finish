package net.virtualvoid.fotofinish

import java.io.File
import java.io.FileFilter

object Main extends App {
  val dir = new File("/home/johannes/Fotos/tmp")

  Scanner.scan(dir)
}

object Scanner {
  val supportedFiles = withExtensions("jpg", "jpeg")

  def scan(directory: File): Unit = {
    val allFiles = allFilesMatching(directory, supportedFiles)

    val numFiles = allFiles.size
    val totalSize = allFiles.map(_.length()).sum

    println(s"Found $numFiles with total size $totalSize")
  }

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

  implicit def predicateAsFileFilter(f: File => Boolean): FileFilter = f(_)
  def byFileName(pred: String => Boolean): FileFilter = f => pred(f.getName)
  def withExtension(ext: String): FileFilter = byFileName(_.endsWith("." + ext))
  def withExtensions(exts: String*): FileFilter = byFileName(name => exts.exists(ext => name.endsWith("." + ext)))
  val isDirectory: FileFilter = _.isDirectory
  val isNoDotDir = byFileName((name: String) => name != ".." && name != ".")

  implicit class RichFileFilter(val filter: FileFilter) extends AnyVal {
    def &&(other: FileFilter): FileFilter = file => filter.accept(file) && other.accept(file)
    def ||(other: FileFilter): FileFilter = file => filter.accept(file) || other.accept(file)
  }
}