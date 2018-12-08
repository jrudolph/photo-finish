package net.virtualvoid.fotofinish.metadata

import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes

import akka.http.scaladsl.model.DateTime
import net.virtualvoid.fotofinish.FileInfo
import spray.json.DefaultJsonProtocol
import spray.json.JsonFormat

import scala.collection.immutable
import scala.reflect.ClassTag

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
object IngestionDataExtractor extends MetadataExtractor {
  type EntryT = IngestionData
  override def kind: String = "ingestion-data"
  override def version: Int = 2
  override def classTag: ClassTag[IngestionData] = implicitly[ClassTag[IngestionData]]

  override protected def extract(file: FileInfo): IngestionData = {
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
  import DefaultJsonProtocol._
  import MetadataJsonProtocol.dateTimeFormat
  override implicit val metadataFormat: JsonFormat[IngestionData] = jsonFormat6(IngestionData)

  override def isCurrent(file: FileInfo, entries: immutable.Seq[MetadataEntry[IngestionData]]): Boolean =
    file.originalFile.forall(original => entries.exists(e => original.getName == e.data.originalFileName))
}