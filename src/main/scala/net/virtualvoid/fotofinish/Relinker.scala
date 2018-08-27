package net.virtualvoid.fotofinish

import java.io.File
import java.nio.file.Files
import java.text.SimpleDateFormat
import java.util.Date

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

    val fileDateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss")

    manager.allFiles
      .foreach { f =>
        val original = f.metadata.get[IngestionData].map(_.originalFileName).getOrElse("unknown.jpg")
        val date = f.metadata.get[ExifBaseData].flatMap(_.dateTaken)

        val fileName = date match {
          case Some(d) =>
            val formattedDate = fileDateFormat.format(new Date(d.clicks))
            s"$formattedDate-$original"
          case None => original
        }

        val dir = dateDir(date)
        val repoPath = f.fileInfo.repoFile.toPath

        def linkTarget(i: Int): Unit = {
          val targetPath = new File(dir, s"${i}_$fileName").toPath

          if (Files.exists(targetPath))
            if (targetPath.toRealPath() == repoPath.toRealPath())
              () // link already there
            else
              linkTarget(i + 1)
          else
            Files.createSymbolicLink(targetPath, repoPath)
        }

        val targetPath = new File(dir, fileName).toPath

        if (Files.exists(targetPath))
          if (targetPath.toRealPath() == repoPath.toRealPath())
            () // link already there
          else
            linkTarget(0)
        else
          Files.createSymbolicLink(targetPath, repoPath)
      }
  }

}
