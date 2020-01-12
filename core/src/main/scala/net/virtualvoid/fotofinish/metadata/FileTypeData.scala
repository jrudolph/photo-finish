package net.virtualvoid.fotofinish.metadata
import java.io.File

import net.virtualvoid.fotofinish.Hash
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
    MetadataExtractor.dep1("net.virtualvoid.fotofinish.metadata.FileTypeDataExtractor", 1, FileTypeData, IngestionData) { (hash, ingestionData, ctx) =>
      ctx.accessDataSync(hash) { f =>
        import sys.process._
        val ext = ingestionData.originalFileName.drop(ingestionData.originalFileName.lastIndexOf('.') + 1)

        val mimeType = s"file -b --mime-type ${f.getAbsolutePath}".!!.trim
        val fileInfo = s"file -b ${f.getAbsolutePath}".!!.trim

        FileTypeData(ext, mimeType, fileInfo)
      }
    }
}

object ImageDataExtractor {
  def DefaultImageMimeTypeFilter: String => Boolean = _.startsWith("image/")

  def apply(_kind: String, _version: Int, metadata: MetadataKind, mimeTypeFilter: String => Boolean = DefaultImageMimeTypeFilter)(f: (Hash, File, ExtractionContext) => Future[metadata.T]): MetadataExtractor =
    MetadataExtractor.cond1(_kind, _version, metadata, FileTypeData)(fileTypeData => if (mimeTypeFilter(fileTypeData.mimeType)) None else Some("Object is not an image")) { (hash, ctx) =>
      ctx.accessData(hash) { file => f(hash, file, ctx) }
    }
  def sync(_kind: String, _version: Int, metadata: MetadataKind, mimeTypeFilter: String => Boolean = DefaultImageMimeTypeFilter)(f: (Hash, File, ExtractionContext) => metadata.T): MetadataExtractor =
    apply(_kind, _version, metadata, mimeTypeFilter)((hash, imageFile, ctx) => Future(f(hash, imageFile, ctx))(ctx.executionContext))

  def fromFileSync(_kind: String, _version: Int, metadata: MetadataKind, mimeTypeFilter: String => Boolean = DefaultImageMimeTypeFilter)(f: File => metadata.T): MetadataExtractor =
    sync(_kind, _version, metadata, mimeTypeFilter)((_, imageFile, _) => f(imageFile))
}