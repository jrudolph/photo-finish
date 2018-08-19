package net.virtualvoid.fotofinish

import java.io.FileInputStream
import java.io.FileOutputStream
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

import scala.collection.immutable
import akka.http.scaladsl.model.DateTime
import spray.json._

import scala.io.Source

final case class MetadataHeader(
    created: DateTime,
    version: Int,
    forData: Hash,
    kind:    String
)

final case class MetadataEntry[T](
    header:    MetadataHeader,
    extractor: MetadataExtractor { type EntryT = T },
    data:      T
)

object MetadataJsonProtocol {
  import DefaultJsonProtocol._
  implicit val dateTimeFormat = new JsonFormat[DateTime] {
    override def read(json: JsValue): DateTime = json match {
      case JsString(data) => DateTime.fromIsoDateTimeString(data).get
    }
    override def write(obj: DateTime): JsValue = JsString(obj.toIsoDateTimeString())
  }

  implicit val hashFormat = new JsonFormat[Hash] {
    override def read(json: JsValue): Hash = json match {
      case JsString(data) => Hash.fromPrefixedString(data).get
    }
    override def write(obj: Hash): JsValue = JsString(obj.toString)
  }
  implicit val metadataHeaderFormat = jsonFormat4(MetadataHeader.apply _)
}

trait MetadataExtractor { thisExtractor =>
  type EntryT
  def kind: String
  def version: Int

  // TODO: support streaming metadata extraction?
  def extractMetadata(file: FileInfo): EntryT

  implicit def metadataFormat: JsonFormat[EntryT]

  import MetadataJsonProtocol._
  import JsonExtra._
  implicit def entryFormat = new JsonFormat[MetadataEntry[EntryT]] {
    override def read(json: JsValue): MetadataEntry[EntryT] = {
      val header = json.convertTo[MetadataHeader]
      require(header.kind == kind && header.version == version)
      val data = json.field("data").convertTo[EntryT]
      MetadataEntry[EntryT](header, thisExtractor, data)
    }
    override def write(obj: MetadataEntry[EntryT]): JsValue =
      obj.header.toJson + ("data" -> obj.data.toJson)
  }

  def get(jsonData: JsValue): MetadataEntry[EntryT] =
    jsonData.convertTo[MetadataEntry[EntryT]]
  def create(entry: MetadataEntry[EntryT]): JsValue =
    entry.toJson
}

object MetadataStore {
  val RegisteredMetadataExtractors: immutable.Seq[MetadataExtractor] = Nil

  def store[T](metadata: MetadataEntry[T], repoConfig: RepositoryConfig): Unit = {
    /*
     - Locate file
     - If exists: append
     - If not: create
     */
    val fos = new FileOutputStream(repoConfig.metadataFile(metadata.header.forData), true)
    val out = new GZIPOutputStream(fos)
    try {
      out.write(metadata.extractor.create(metadata).compactPrint.getBytes("utf8"))
      out.write('\n')
    } finally out.close()
  }

  def load(target: FileInfo): immutable.Seq[MetadataEntry[_]] = {
    if (!target.metadataFile.exists()) Nil
    else
      Source.fromInputStream(new GZIPInputStream(new FileInputStream(target.metadataFile))).getLines().flatMap(readMetadataEntry).toVector
  }

  def readMetadataEntry(entry: String): Option[MetadataEntry[_]] = {
    import MetadataJsonProtocol._
    val jsonValue = entry.parseJson
    val header = jsonValue.convertTo[MetadataHeader]
    findExtractor(header).map(_.get(jsonValue))
  }

  def findExtractor(header: MetadataHeader): Option[MetadataExtractor] =
    RegisteredMetadataExtractors.find(e => e.kind == header.kind && e.version == header.version)
}

object JsonExtra {
  implicit class RichJsValue(val jsValue: JsValue) extends AnyVal {
    def +(field: (String, JsValue)): JsObject = {
      val obj = jsValue.asJsObject
      obj.copy(fields = obj.fields + field)
    }
    def field(name: String): JsValue =
      jsValue.asJsObject.fields(name)
  }
}