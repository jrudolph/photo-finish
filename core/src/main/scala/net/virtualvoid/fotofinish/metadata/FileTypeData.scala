package net.virtualvoid.fotofinish.metadata
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