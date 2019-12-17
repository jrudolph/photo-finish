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
    kind:    String,
    seqNr:   Long     = -1
) {
  def withSeqNr(newSeqNr: Long): MetadataHeader = copy(seqNr = newSeqNr)
}

trait MetadataEntry {
  type T
  def header: MetadataHeader
  def extractor: MetadataExtractor.Aux[T]
  def data: T

  def withSeqNr(newSeqNr: Long): MetadataEntry.Aux[T]
}
object MetadataEntry {
  type Aux[_T] = MetadataEntry { type T = _T }

  def apply[T](header: MetadataHeader, extractor: MetadataExtractor.Aux[T], data: T): Aux[T] =
    EntryImpl(header, extractor, data)

  def unapply[T: ClassTag](entry: MetadataEntry): Option[(MetadataHeader, MetadataExtractor.Aux[T], T)] =
    entry match {
      case EntryImpl(header, extractor, data: T) => Some((header, extractor.asInstanceOf[MetadataExtractor.Aux[T]], data))
      case _                                     => None
    }

  private case class EntryImpl[_T](
      header:    MetadataHeader,
      extractor: MetadataExtractor.Aux[_T],
      data:      _T
  ) extends MetadataEntry {
    type T = _T

    override def withSeqNr(newSeqNr: Long): Aux[_T] = copy(header = header.withSeqNr(newSeqNr))
  }
}

object MetadataJsonProtocol {
  def error(message: String): Nothing = throw DeserializationException(message)

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
  implicit val metadataHeaderFormat = jsonFormat5(MetadataHeader.apply _)
}

trait MetadataExtractor { thisExtractor =>
  type EntryT
  def kind: String
  def version: Int
  implicit def classTag: ClassTag[EntryT]

  /** Override to check allow new entries created for the same version */
  def isCurrent(file: FileInfo, entries: immutable.Seq[MetadataEntry.Aux[EntryT]]): Boolean = true

  /**
   * Determines if an entry is correct or should be filtered out.
   *
   * Override to filter out entries that might have been accidentally or wrongly created.
   */
  def isCorrect(entry: MetadataEntry.Aux[EntryT]): Boolean = true

  // TODO: support streaming metadata extraction?
  def extractMetadata(file: FileInfo): Try[MetadataEntry.Aux[EntryT]] =
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
  private implicit val entryFormat = new JsonFormat[MetadataEntry.Aux[EntryT]] {
    override def read(json: JsValue): MetadataEntry.Aux[EntryT] = {
      val header = json.convertTo[MetadataHeader]
      require(header.kind == kind && header.version == version)
      val data = json.field("data").convertTo[EntryT]
      MetadataEntry[EntryT](header, thisExtractor, data)
    }
    override def write(obj: MetadataEntry.Aux[EntryT]): JsValue =
      obj.header.toJson + ("data" -> obj.data.toJson)
  }

  def get(jsonData: JsValue): MetadataEntry.Aux[EntryT] =
    jsonData.convertTo[MetadataEntry.Aux[EntryT]]
  def create(entry: MetadataEntry.Aux[EntryT]): JsValue =
    entry.toJson
}
object MetadataExtractor {
  type Aux[T] = MetadataExtractor { type EntryT = T }
}

final case class Metadata(entries: immutable.Seq[MetadataEntry]) {
  def getEntry[E: ClassTag]: Option[MetadataEntry.Aux[E]] =
    entries.reverse.collectFirst {
      case e @ MetadataEntry(_, _, _: E) => e.asInstanceOf[MetadataEntry.Aux[E]]
    }
  def getEntries[E: ClassTag]: immutable.Seq[MetadataEntry.Aux[E]] =
    entries.collect {
      case e @ MetadataEntry(_, _, _: E) => e.asInstanceOf[MetadataEntry.Aux[E]]
    }
  def getValues[E: ClassTag]: immutable.Seq[E] = getEntries[E].map(_.data)

  def get[T: ClassTag]: Option[T] = getEntry[T].map(_.data)

  def get[T](shortcut: MetadataShortcuts.ShortCut[T]): T = shortcut(this)
}
