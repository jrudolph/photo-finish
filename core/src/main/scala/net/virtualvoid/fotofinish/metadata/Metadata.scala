package net.virtualvoid.fotofinish
package metadata

import java.io.File
import java.util.Base64

import akka.http.scaladsl.model.DateTime
import akka.util.ByteString
import net.virtualvoid.fotofinish.metadata.Id.Hashed
import spray.json._
import util.JsonExtra._

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.ClassTag
import scala.util.Try

trait MetadataKind {
  type T
  def kind: String
  def version: Int
  implicit def classTag: ClassTag[T]
  implicit def jsonFormat: JsonFormat[T]

  /*  implicit def entryFormat: JsonFormat[MetadataEntry2.Aux[T]] = {
    import DefaultJsonProtocol._
    // subset of all headers
    case class KindedEntry(kind: MetadataKind)
    implicit def kindedFormat: JsonFormat[KindedEntry] = jsonFormat1(KindedEntry.apply _)
    implicit def entryTFormat: JsonFormat[MetadataEntry2.Impl[T]] = jsonFormat

    new JsonFormat[MetadataEntry2.Aux[T]] {
      override def read(json: JsValue): MetadataEntry2.Aux[T] = {
        val jsonKind = json.convertTo[KindedEntry].kind
        require(jsonKind.kind == kind && jsonKind.version == version)
        val data = json.field("data").convertTo[EntryT]
        MetadataEntry[EntryT](jsonKind, thisExtractor, data)
      }
      override def write(obj: MetadataEntry.Aux[EntryT]): JsValue =
        obj.header.toJson + ("data" -> obj.data.toJson)
    }
  }*/
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

object Creator {
  import DefaultJsonProtocol._
  import util.JsonExtra._
  implicit val extractorFormat: JsonFormat[Extractor] = jsonFormat2(Extractor.apply _)
  implicit val creatorFormat: JsonFormat[Creator] = new JsonFormat[Creator] {
    override def read(json: JsValue): Creator = json.asJsObject.field("type") match {
      case JsString("Ingestion") => Ingestion
      case JsString("Extractor") => json.convertTo[Extractor]
    }
    override def write(obj: Creator): JsValue = obj match {
      case Ingestion    => JsObject("type" -> JsString("Ingestion"))
      case e: Extractor => e.toJson + ("type" -> JsString("Extractor"))
    }
  }
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
sealed trait Id
object Id {
  final case class Hashed(hash: Hash) extends Id
  // final case class ByUUID(uuid: UUID) extends Id

  import DefaultJsonProtocol._
  implicit def hashedFormat: JsonFormat[Hashed] = jsonFormat1(Hashed.apply _)
  implicit def idFormat: JsonFormat[Id] = new JsonFormat[Id] {
    override def write(obj: Id): JsValue = obj match {
      case h: Hashed => h.toJson + ("type" -> JsString("hashed"))
    }
    override def read(json: JsValue): Id =
      json.asJsObject.field("type") match {
        case JsString("hashed") => json.convertTo[Hashed]
      }
  }

  implicit class IdExtension(val id: Id) extends AnyVal {
    def hash: Hash = id.asInstanceOf[Hashed].hash
  }
}

trait MetadataEntry2 {
  type T
  def target: Id
  def secondaryTargets: Vector[Id]
  def kind: MetadataKind.Aux[T]
  def creation: CreationInfo
  def value: T
}
object MetadataEntry2 {
  type Aux[_T] = MetadataEntry2 { type T = _T }

  def apply[T](target: Id, secondaryTargets: Vector[Id], kind: MetadataKind.Aux[T], creation: CreationInfo, value: T): Aux[T] =
    Impl(target, secondaryTargets, kind, creation, value)

  def unapply[T: ClassTag](entry: MetadataEntry2): Option[T] =
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
  ) extends MetadataEntry2 {
    type T = _T
  }

  def entryFormat(knownKinds: Set[MetadataKind]): JsonFormat[MetadataEntry2] = {
    case class SimpleEntry(kind: KindHeader)
    case class KindHeader(kind: String, version: Int)

    import DefaultJsonProtocol._
    implicit val headerFormat = jsonFormat2(KindHeader.apply _)
    implicit val simpleEntryFormat = jsonFormat1(SimpleEntry.apply _)

    implicit def kindFormatGen: JsonFormat[MetadataKind] = new JsonFormat[MetadataKind] {
      override def read(json: JsValue): MetadataKind = {
        val kindHeader = json.convertTo[KindHeader]
        knownKinds.find(k => k.kind == kindHeader.kind && k.version == kindHeader.version)
          .getOrElse(throw new IllegalArgumentException(s"No MetadataKind found for [$kindHeader] (has [${knownKinds.mkString(", ")}])"))
      }
      override def write(obj: MetadataKind): JsValue =
        KindHeader(obj.kind, obj.version).toJson
    }
    implicit def kindFormat[T]: JsonFormat[MetadataKind.Aux[T]] = kindFormatGen.asInstanceOf[JsonFormat[MetadataKind.Aux[T]]]
    implicit def implFormat(kind: MetadataKind): JsonFormat[Impl[kind.T]] = {
      import kind.jsonFormat
      jsonFormat5(Impl.apply)
    }

    new JsonFormat[MetadataEntry2] {
      override def write(obj: MetadataEntry2): JsValue =
        obj.asInstanceOf[Impl[obj.T]].toJson(implFormat(obj.kind))
      override def read(json: JsValue): MetadataEntry2 = {
        val kind = json.asJsObject.field("kind").convertTo[MetadataKind]
        json.convertTo(implFormat(kind))
      }
    }
  }
}

trait MetadataEnvelope {
  def seqNr: Long
  def entry: MetadataEntry2
}
object MetadataEnvelope {
  def apply(seqNr: Long, entry: MetadataEntry2): MetadataEnvelope =
    Impl(seqNr, entry)

  private case class Impl(seqNr: Long, entry: MetadataEntry2) extends MetadataEnvelope

  import DefaultJsonProtocol._
  implicit def envelopeFormat(implicit entryFormat: JsonFormat[MetadataEntry2]): JsonFormat[MetadataEnvelope] = {
    implicit val implFormat: JsonFormat[Impl] = jsonFormat2(Impl.apply _)

    new JsonFormat[MetadataEnvelope] {
      override def read(json: JsValue): MetadataEnvelope = json.convertTo[Impl]
      override def write(obj: MetadataEnvelope): JsValue = obj.asInstanceOf[Impl].toJson
    }
  }
}

trait ExtractionContext {
  implicit def executionContext: ExecutionContext
  def accessData[T](hash: Hash)(f: File => Future[T]): Future[T]
  def accessDataSync[T](hash: Hash)(f: File => T): Future[T] = accessData(hash)(file => Future.fromTry(Try(f(file))))
}
trait MetadataExtractor2 {
  type EntryT
  def kind: String
  def version: Int
  def metadataKind: MetadataKind.Aux[EntryT]

  final def extract(hash: Hash, ctx: ExtractionContext): Future[MetadataEntry2.Aux[EntryT]] =
    extractEntry(hash, ctx)
      .map(value =>
        MetadataEntry2(
          Hashed(hash),
          Vector.empty,
          metadataKind,
          CreationInfo(DateTime.now, inferred = true, Extractor(kind, version)),
          value))(ctx.executionContext)

  protected def extractEntry(hash: Hash, ctx: ExtractionContext): Future[EntryT]
}

/*final case class MetadataHeader(
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

  def seqNr: Long = header.seqNr
  def withSeqNr(newSeqNr: Long): MetadataEntry.Aux[T]
}*/
/*object MetadataEntry {
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
}*/

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
}

/*trait MetadataExtractor { thisExtractor =>
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
}*/

final case class Metadata(entries: immutable.Seq[MetadataEntry2]) {
  def getEntry[E: ClassTag]: Option[MetadataEntry2.Aux[E]] =
    entries.reverse.collectFirst {
      case e @ MetadataEntry2(_: E) => e.asInstanceOf[MetadataEntry2.Aux[E]]
    }
  def getEntries[E: ClassTag]: immutable.Seq[MetadataEntry2.Aux[E]] =
    entries.collect {
      case e @ MetadataEntry2(_: E) => e.asInstanceOf[MetadataEntry2.Aux[E]]
    }
  def getValues[E: ClassTag]: immutable.Seq[E] = getEntries[E].map(_.value)

  def get[T: ClassTag]: Option[T] = getEntry[T].map(_.value)

  def get[T](shortcut: MetadataShortcuts.ShortCut[T]): T = shortcut(this)
}
