package net.virtualvoid.fotofinish.metadata
import java.io.File

import net.virtualvoid.fotofinish.Hash
import net.virtualvoid.fotofinish.metadata.MetadataKind.Aux
import spray.json.JsonFormat

import scala.concurrent.Future

final case class FileTypeData(
    extension: String,
    mimeType:  String,
    fileInfo:  String
)
object FileTypeData extends MetadataKind.Impl[FileTypeData]("net.virtualvoid.fotofinish.metadata.FileTypeData", 1) {
  import spray.json.DefaultJsonProtocol._
  def jsonFormat: JsonFormat[FileTypeData] = jsonFormat3(FileTypeData.apply)
}

object FileTypeDataExtractor {
  def instance: MetadataExtractor =
    new MetadataExtractor {
      type EntryT = FileTypeData
      def kind: String = "net.virtualvoid.fotofinish.metadata.FileTypeDataExtractor"
      def version: Int = 3

      def metadataKind: Aux[FileTypeData] = FileTypeData
      def dependsOn: Vector[MetadataKind] = Vector(IngestionData)

      protected def extractEntry(hash: Hash, dependencies: Vector[MetadataEntry], ctx: ExtractionContext): Future[FileTypeData] =
        ctx.accessDataSync(hash) { f =>
          val ingestionData = dependencies(0).cast(IngestionData).value
          import sys.process._
          val ext = ingestionData.originalFileName.drop(ingestionData.originalFileName.lastIndexOf('.') + 1)

          val mimeType = s"file -E -b --mime-type ${f.getAbsolutePath}".!!.trim
          val fileInfo = s"file -E -b ${f.getAbsolutePath}".!!.trim

          FileTypeData(ext, mimeType, fileInfo)
        }

      override def upgradeExisting(existing: MetadataEntry.Aux[FileTypeData], dependencies: Vector[MetadataEntry]): MetadataExtractor.Upgrade = {
        val value = existing.value
        // previous version was broken because it didn't use the `-E` flag so the result might contain just the error message
        if (value.mimeType.contains("cannot open") || value.fileInfo.contains("cannot open")) MetadataExtractor.RerunExtractor
        else MetadataExtractor.Keep
      }
    }

  /*MetadataExtractor.dep1("net.virtualvoid.fotofinish.metadata.FileTypeDataExtractor", 1, FileTypeData, IngestionData) { (hash, ingestionData, ctx) =>
      ctx.accessDataSync(hash) { f =>
        import sys.process._
        val ext = ingestionData.originalFileName.drop(ingestionData.originalFileName.lastIndexOf('.') + 1)

        val mimeType = s"file -E -b --mime-type ${f.getAbsolutePath}".!!.trim
        val fileInfo = s"file -E -b ${f.getAbsolutePath}".!!.trim

        FileTypeData(ext, mimeType, fileInfo)
      }
    }*/
}

object ImageDataExtractor {
  def DefaultImageMimeTypeFilter: String => Boolean = _.startsWith("image/")

  def apply(_kind: String, _version: Int, metadata: MetadataKind, mimeTypeFilter: String => Boolean = DefaultImageMimeTypeFilter)(f: (Hash, File, ExtractionContext) => Future[metadata.T]): MetadataExtractor =
    MetadataExtractor.cond1(_kind, _version, metadata, FileTypeData)(fileTypeData => if (mimeTypeFilter(fileTypeData.mimeType)) None else Some(s"Object is not an image but [${fileTypeData.mimeType}]")) { (hash, ctx) =>
      ctx.accessData(hash) { file => f(hash, file, ctx) }
    }
  def sync(_kind: String, _version: Int, metadata: MetadataKind, mimeTypeFilter: String => Boolean = DefaultImageMimeTypeFilter)(f: (Hash, File, ExtractionContext) => metadata.T): MetadataExtractor =
    apply(_kind, _version, metadata, mimeTypeFilter)((hash, imageFile, ctx) => Future(f(hash, imageFile, ctx))(ctx.executionContext))

  def fromFileSync(_kind: String, _version: Int, metadata: MetadataKind, mimeTypeFilter: String => Boolean = DefaultImageMimeTypeFilter)(f: File => metadata.T): MetadataExtractor =
    sync(_kind, _version, metadata, mimeTypeFilter)((_, imageFile, _) => f(imageFile))
}