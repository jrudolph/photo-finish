package net.virtualvoid.fotofinish
package metadata

import java.util.Base64

import akka.http.scaladsl.model.DateTime
import akka.util.ByteString
import net.virtualvoid.fotofinish.util.JsonExtra
import spray.json._

import scala.collection.immutable
import scala.reflect.ClassTag

trait MetadataKind {
  type T
  def kind: String
  def version: Int
  implicit def classTag: ClassTag[T]
  implicit def jsonFormat: JsonFormat[T]
}
object MetadataKind {
  type Aux[_T] = MetadataKind { type T = _T }

  abstract class Impl[_T](val kind: String, val version: Int)(implicit tTag: ClassTag[_T]) extends MetadataKind {
    type T = _T
    implicit def classTag: ClassTag[T] = tTag
  }
}
sealed trait Creator
// final case class CreatedByUser(userName: String) extends Creator
// final case class Imported()
case object Ingestion extends Creator
case class Extractor(
    id:      String,
    version: Int
// machineId: String
) extends Creator
case object Deleted extends Creator

object Creator {
  import DefaultJsonProtocol._
  import util.JsonExtra._
  implicit val extractorFormat: JsonFormat[Extractor] = jsonFormat2(Extractor.apply _)
  implicit val creatorFormat: JsonFormat[Creator] = new JsonFormat[Creator] {
    override def read(json: JsValue): Creator = json.asJsObject.field("type") match {
      case JsString("Ingestion") => Ingestion
      case JsString("Deleted")   => Ingestion
      case JsString("Extractor") => json.convertTo[Extractor]
      case x                     => MetadataJsonProtocol.error(s"Cannot read Creator from $x")
    }
    override def write(obj: Creator): JsValue = obj match {
      case Ingestion    => JsObject("type" -> JsString("Ingestion"))
      case Deleted      => JsObject("type" -> JsString("Deleted"))
      case e: Extractor => e.toJson + ("type" -> JsString("Extractor"))
    }
  }
  def fromExtractor(extractor: MetadataExtractor): Creator = Extractor(extractor.kind, extractor.version)
}

case class CreationInfo(
    created:  DateTime,
    inferred: Boolean, // = can be recreated automatically
    creator:  Creator // by user, by extractor, by other process
)
object CreationInfo {
  import DefaultJsonProtocol._
  implicit val dtF: JsonFormat[DateTime] = MetadataJsonProtocol.dateTimeFormat
  implicit val creationInfo: JsonFormat[CreationInfo] = jsonFormat3(CreationInfo.apply _)
}
sealed trait Id {
  def kind: String
  def stringRepr: String

  def idString: String = s"$kind:$stringRepr"
}
object Id {
  final case class Hashed(hash: Hash) extends Id {
    def kind: String = hash.hashAlgorithm.name
    def stringRepr: String = hash.asHexString

    override def hashCode(): Int = hash.hashCode()
    override def toString: String = hash.toString
  }
  // final case class ByUUID(uuid: UUID) extends Id

  def generic(_kind: String, repr: String): Id =
    new Id {
      def kind: String = _kind
      def stringRepr: String = repr
    }

  def fromString(idStr: String): Id =
    Hashed(Hash.fromPrefixedString(idStr).getOrElse(MetadataJsonProtocol.error(s"Cannot read Id from '$idStr'")))

  import DefaultJsonProtocol._
  implicit def hashedFormat: JsonFormat[Hashed] = jsonFormat1(Hashed.apply _)
  implicit def idFormat: JsonFormat[Id] = new JsonFormat[Id] {
    override def write(obj: Id): JsValue = JsString(obj.idString)
    override def read(json: JsValue): Id = json match {
      case JsString(x) => Id.fromString(x)
      case x           => MetadataJsonProtocol.error(s"Cannot read Id from $x")
    }
  }

  implicit class IdExtension(val id: Id) extends AnyVal {
    def hash: Hash = id.asInstanceOf[Hashed].hash
  }
  implicit val idOrdering: Ordering[Id] = Ordering.by(_.idString)
}

trait MetadataEntry {
  type T
  def target: Id
  def secondaryTargets: Vector[Id]
  def kind: MetadataKind.Aux[T]
  def creation: CreationInfo
  def value: T

  def cast[U](candidateKind: MetadataKind.Aux[U]): MetadataEntry.Aux[U] =
    if (kind == candidateKind) this.asInstanceOf[MetadataEntry.Aux[U]]
    else throw new RuntimeException(s"Entry of kind $kind cannot be cast to $candidateKind")
}
object MetadataEntry {
  type Aux[_T] = MetadataEntry { type T = _T }

  def apply[T](target: Id, secondaryTargets: Vector[Id], kind: MetadataKind.Aux[T], creation: CreationInfo, value: T): Aux[T] =
    Impl(target, secondaryTargets, kind, creation, value)

  def unapply[T: ClassTag](entry: MetadataEntry): Option[T] =
    entry match {
      case Impl(_, _, _, _, value: T) => Some(value)
      case _                          => None
    }

  private[metadata] case class Impl[_T](
      target:           Id,
      secondaryTargets: Vector[Id],
      kind:             MetadataKind.Aux[_T],
      creation:         CreationInfo,
      value:            _T
  ) extends MetadataEntry {
    type T = _T
  }

  implicit def entryAuxFormat[T](implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[Aux[T]] = entryFormat.asInstanceOf[JsonFormat[Aux[T]]]
}

trait MetadataEnvelope {
  def seqNr: Long
  def entry: MetadataEntry
}
object MetadataEnvelope {
  def apply(seqNr: Long, entry: MetadataEntry): MetadataEnvelope =
    Impl(seqNr, entry)

  private case class Impl(seqNr: Long, entry: MetadataEntry) extends MetadataEnvelope

  import DefaultJsonProtocol._
  implicit def envelopeFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[MetadataEnvelope] =
    JsonExtra.deriveFormatFrom[Impl].to[MetadataEnvelope](_.asInstanceOf[Impl], identity)(jsonFormat2(Impl.apply _))
}

object MetadataJsonProtocol {
  def error(message: String): Nothing = throw DeserializationException(message)

  import DefaultJsonProtocol._
  implicit val dateTimeFormat: JsonFormat[DateTime] =
    JsonExtra.deriveFormatFrom[String](
      _.toIsoDateTimeString(),
      data => DateTime.fromIsoDateTimeString(data).getOrElse(error(s"Date could not be read [$data]"))
    )

  implicit val byteStringFormat: JsonFormat[ByteString] =
    JsonExtra.deriveFormatFrom[String](
      bytes => Base64.getEncoder.encodeToString(bytes.toArray),
      base64Str => ByteString.fromArray(Base64.getDecoder.decode(base64Str)))

  case class SimpleKind(
      kind:    String,
      version: Int
  )
  case class SimpleEntry(
      target:           Id,
      secondaryTargets: Vector[Id],
      kind:             SimpleKind,
      creation:         CreationInfo,
      value:            JsValue
  )
  object SimpleEntry {
    def apply(metadataEntry: MetadataEntry): SimpleEntry =
      SimpleEntry(
        metadataEntry.target,
        metadataEntry.secondaryTargets,
        SimpleKind(metadataEntry.kind.kind, metadataEntry.kind.version),
        metadataEntry.creation,
        metadataEntry.value.toJson(metadataEntry.kind.jsonFormat)
      )
  }
  case class SimpleJournalEntry(seqNr: Long, entry: SimpleEntry)

  implicit def simpleKindFormat: JsonFormat[SimpleKind] = jsonFormat2(SimpleKind.apply)
  implicit def simpleEntryFormat: JsonFormat[SimpleEntry] = jsonFormat5(SimpleEntry.apply)
  implicit def simpleJournalEntryFormat: JsonFormat[SimpleJournalEntry] = jsonFormat2(SimpleJournalEntry.apply)

  def entryFormat(resolveKind: (String, Int) => MetadataKind): JsonFormat[MetadataEntry] =
    JsonExtra.deriveFormatFrom[SimpleEntry](SimpleEntry(_), resolveEntry(resolveKind))
  def envelopeFormat(resolveKind: (String, Int) => MetadataKind): JsonFormat[MetadataEnvelope] =
    MetadataEnvelope.envelopeFormat(entryFormat(resolveKind))

  private def resolveEntry(resolveKind: (String, Int) => MetadataKind): SimpleEntry => MetadataEntry = { entry =>
    val kind = resolveKind(entry.kind.kind, entry.kind.version)

    MetadataEntry[kind.T](
      entry.target,
      entry.secondaryTargets,
      kind,
      entry.creation,
      entry.value.convertTo[kind.T](kind.jsonFormat)
    )
  }
}

trait EntryFormats {
  protected def resolveKind(kind: String, version: Int): MetadataKind

  implicit def entryFormat: JsonFormat[MetadataEntry] =
    MetadataJsonProtocol.entryFormat(resolveKind)
  implicit def envelopeFormat: JsonFormat[MetadataEnvelope] =
    MetadataJsonProtocol.envelopeFormat(resolveKind)
}

trait EntryFormatsFromKinds extends EntryFormats {
  protected def allKinds: Set[MetadataKind]

  private lazy val kindMap: Map[(String, Int), MetadataKind] =
    allKinds.map(k => (k.kind, k.version) -> k).toMap

  override def resolveKind(kind: String, version: Int): MetadataKind =
    kindMap.getOrElse(
      kind -> version,
      throw new IllegalArgumentException(s"No MetadataKind found for [$kind] (has [${allKinds.mkString(", ")}])"))
}

final case class Metadata(entries: immutable.Seq[MetadataEntry]) {
  def getEntry[E: ClassTag]: Option[MetadataEntry.Aux[E]] =
    entries.reverse.collectFirst {
      case e @ MetadataEntry(_: E) => e.asInstanceOf[MetadataEntry.Aux[E]]
    }
  def getEntries[E: ClassTag]: immutable.Seq[MetadataEntry.Aux[E]] =
    entries.collect {
      case e @ MetadataEntry(_: E) => e.asInstanceOf[MetadataEntry.Aux[E]]
    }
  def getValues[E: ClassTag]: immutable.Seq[E] = getEntries[E].map(_.value)

  def get[T: ClassTag]: Option[T] = getEntry[T].map(_.value)
}
