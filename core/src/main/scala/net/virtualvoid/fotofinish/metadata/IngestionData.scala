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

/*object IngestionDataExtractor /*extends MetadataExtractor*/ {
  type EntryT = IngestionData
  def kind: String = "ingestion-data"
  def version: Int = 2
  def classTag: ClassTag[IngestionData] = implicitly[ClassTag[IngestionData]]

  protected def extract(file: FileInfo): IngestionData = {
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
  implicit val metadataFormat: JsonFormat[IngestionData] = jsonFormat6(IngestionData)

  /*def isCurrent(file: FileInfo, entries: immutable.Seq[MetadataEntry.Aux[IngestionData]]): Boolean =
    file.originalFile.forall { original =>
      entries.exists(e =>
        original.getName == e.data.originalFileName &&
          original.getParent == e.data.originalFilePath
      )
    }

  def isCorrect(entry: MetadataEntry.Aux[IngestionData]): Boolean =
    // the extractor previously accidentally ran against repo files so we use a simple heuristic here to be able to filter
    // out these entries
    !entry.data.originalFilePath.contains("/by-sha-512/")*/
}*/
