package net.virtualvoid.fotofinish

import java.io.File
import java.nio.file.Files

import akka.http.scaladsl.model.DateTime

object Relinker {
  def createLinkedDirByYearMonth(manager: RepositoryManager): Unit = {
    def dateDir(dateTaken: Option[DateTime]): File = {
      val sub = dateTaken match {
        case None       => "unknown"
        case Some(date) => f"${date.year}%04d/${date.month}%02d"
      }
      val subDir = new File(manager.config.storageDir, "by-date")
      val res = new File(subDir, sub)
      res.mkdirs()
      res
    }

    manager.allFiles
      .foreach { f =>
        val original = f.metadata.get[IngestionData].map(_.originalFileName).getOrElse("unknown.jpeg")
        val date = f.metadata.get[ExifBaseData].flatMap(_.dateTaken)
        val dir = dateDir(date)
        Files.createLink(new File(dir, original).toPath, f.fileInfo.repoFile.toPath)
      }
  }

}
