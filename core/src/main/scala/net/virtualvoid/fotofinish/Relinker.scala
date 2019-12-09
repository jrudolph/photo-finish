package net.virtualvoid.fotofinish

import java.io.File
import java.nio.file.Files
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.immutable

import akka.http.scaladsl.model.DateTime

import metadata._

object Relinker {
  def byOriginalFileName(manager: RepositoryManager)(fileAndMetadata: FileAndMetadata): immutable.Seq[File] = {
    val parentDir = new File(manager.config.linkRootDir, "by-original-name")

    MetadataShortcuts.OriginalFullFilePaths(fileAndMetadata.metadata)
      .map { p =>
        if (p.startsWith("/")) p.drop(1)
        else p
      }.map { p =>
        new File(parentDir, p)
      }
  }

  def byYearMonth(manager: RepositoryManager)(fileAndMetadata: FileAndMetadata): immutable.Seq[File] = {
    def dateDir(dateTaken: Option[DateTime]): File = {
      val sub = dateTaken match {
        case None       => "unknown"
        case Some(date) => f"${date.year}%04d/${date.month}%02d"
      }
      val subDir = new File(manager.config.linkRootDir, "by-date")
      val res = new File(subDir, sub)
      res.mkdirs()
      res
    }

    val fileDateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss")

    val original = fileAndMetadata.metadata.get[IngestionData].map(_.originalFileName).getOrElse("unknown.jpg")
    val date = fileAndMetadata.metadata.get[ExifBaseData].flatMap(_.dateTaken)

    val fileName = date match {
      case Some(d) =>
        val formattedDate = fileDateFormat.format(new Date(d.clicks))
        s"$formattedDate-$original"
      case None =>
        val hash = fileAndMetadata.fileInfo.hash.asHexString.take(20)
        s"$hash-$original"
    }

    val dir = dateDir(date)
    new File(dir, fileName) :: Nil
  }
  def createDirStructure(manager: RepositoryManager)(locator: FileAndMetadata => immutable.Seq[File]): Unit = {
    manager.allRepoFiles()
      .zipWithIndex
      .grouped(1000)
      .foreach { group =>
        group.toVector.par
          .map {
            case (fileInfo, i) => FileAndMetadata(fileInfo, manager.metadataFor(fileInfo.hash)) -> i
          }
          .foreach {
            case (f, idx) =>
              if ((idx % 1000) == 0) println(s"At $idx")

              val repoPath = f.fileInfo.repoFile.toPath
              locator(f).foreach(createLink)

              def createLink(targetFile: File): Unit = {

                val fileName = targetFile.getName
                val dir = targetFile.getParent
                targetFile.getParentFile.mkdirs()

                def linkTarget(i: Int): Unit = {
                  val targetPath =
                    if (i == 0) targetFile.toPath
                    else new File(dir, s"${fileName}_${i}").toPath

                  if (Files.exists(targetPath))
                    if (targetPath.toRealPath() == repoPath.toRealPath())
                      () // link already there
                    else {
                      println(s"File already exists in dir at [$targetPath] but is no link to [${repoPath.toRealPath()}] but to [${targetPath.toRealPath()}]")
                      linkTarget(i + 1)
                    }
                  else
                    Files.createSymbolicLink(targetPath, repoPath)
                }

                linkTarget(0)
              }
          }
      }
  }

}
