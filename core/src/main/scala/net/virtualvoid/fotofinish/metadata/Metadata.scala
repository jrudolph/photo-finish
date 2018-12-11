package net.virtualvoid.fotofinish
package metadata

import java.util.Base64

import akka.http.scaladsl.model.DateTime
import akka.util.ByteString
import spray.json._

import scala.collection.immutable
import scala.reflect.ClassTag
import scala.util.Try

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
  def error(message: String): Nothing =
    throw new DeserializationException(message)

  import DefaultJsonProtocol._
  implicit val dateTimeFormat = new JsonFormat[DateTime] {
    override def read(json: JsValue): DateTime = json match {
      case JsString(data) => DateTime.fromIsoDateTimeString(data).getOrElse(error(s"Date could not be read [$data]"))
    }
    override def write(obj: DateTime): JsValue = JsString(obj.toIsoDateTimeString())
  }
  implicit val byteStringFormat = new JsonFormat[ByteString] {
    override def read(json: JsValue): ByteString = json match {
      case JsString(data) => ByteString.fromArray(Base64.getDecoder.decode(data))
    }
    override def write(obj: ByteString): JsValue = {
      val data = Base64.getEncoder.encodeToString(obj.toArray)
      JsString(data)
    }
  }

  implicit val hashFormat = new JsonFormat[Hash] {
    override def read(json: JsValue): Hash = json match {
      case JsString(data) => Hash.fromPrefixedString(data).getOrElse(error(s"Prefixed hash string could not be read [$data]"))
    }
    override def write(obj: Hash): JsValue = JsString(obj.toString)
  }
  implicit val metadataHeaderFormat = jsonFormat4(MetadataHeader.apply _)
}

trait MetadataExtractor { thisExtractor =>
  type EntryT
  def kind: String
  def version: Int
  implicit def classTag: ClassTag[EntryT]

  /** Override to check allow new entries created for the same version */
  def isCurrent(file: FileInfo, entries: immutable.Seq[MetadataEntry[EntryT]]): Boolean = true

  /**
   * Determines if an entry is correct or should be filtered out.
   *
   * Override to filter out entries that might have been accidentally or wrongly created.
   */
  def isCorrect(entry: MetadataEntry[EntryT]): Boolean = true

  // TODO: support streaming metadata extraction?
  def extractMetadata(file: FileInfo): Try[MetadataEntry[EntryT]] =
    Try {
      MetadataEntry[EntryT](
        MetadataHeader(
          DateTime.now,
          version,
          file.hash,
          kind
        ),
        this,
        extract(file)
      )
    }

  protected def extract(file: FileInfo): EntryT

  implicit def metadataFormat: JsonFormat[EntryT]

  import MetadataJsonProtocol._
  import util.JsonExtra._
  private implicit val entryFormat = new JsonFormat[MetadataEntry[EntryT]] {
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

final case class Metadata(entries: immutable.Seq[MetadataEntry[_]]) {
  def getEntry[E: ClassTag]: Option[MetadataEntry[E]] =
    entries.reverse.collectFirst {
      case e @ MetadataEntry(_, _, data: E) => e.asInstanceOf[MetadataEntry[E]]
    }
  def getEntries[E: ClassTag]: immutable.Seq[MetadataEntry[E]] =
    entries.collect {
      case e @ MetadataEntry(_, _, data: E) => e.asInstanceOf[MetadataEntry[E]]
    }
  def getValues[E: ClassTag]: immutable.Seq[E] = getEntries[E].map(_.data)

  def get[T: ClassTag]: Option[T] = getEntry[T].map(_.data)

  def get[T](shortcut: MetadataShortcuts.ShortCut[T]): T = shortcut(this)
}
