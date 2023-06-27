package net.virtualvoid.fotofinish.metadata

import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes

import org.apache.pekko.http.scaladsl.model.DateTime
import net.virtualvoid.fotofinish.FileInfo
import spray.json.{ DefaultJsonProtocol, JsonFormat }

final case class IngestionData(
    fileSize:                 Long,
    originalFileName:         String,
    originalFilePath:         String,
    originalFileCreationDate: DateTime,
    originalFileModifiedDate: DateTime,
    repoFileModifiedDate:     DateTime
) {
  def originalFullFilePath: String = originalFilePath + "/" + originalFileName
}
object IngestionData extends MetadataKind.Impl[IngestionData]("net.virtualvoid.fotofinish.metadata.IngestionData", 1) {
  import DefaultJsonProtocol._
  import MetadataJsonProtocol.dateTimeFormat
  def jsonFormat: JsonFormat[IngestionData] = jsonFormat6(IngestionData.apply _)

  def fromFileInfo(file: FileInfo): IngestionData = {
    val original = file.originalFile.getOrElse(throw new RuntimeException("Cannot extract IngestionData when original file info is not available"))
    IngestionData(
      original.length(),
      original.getName,
      original.getParent,
      DateTime(Files.readAttributes(original.toPath, classOf[BasicFileAttributes]).creationTime().toMillis),
      DateTime(original.lastModified()),
      DateTime(file.repoFile.lastModified())
    )
  }
}
