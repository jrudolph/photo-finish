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
      case x                     => MetadataJsonProtocol.error(s"Cannot read Creator from $x")
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
sealed trait Id {
  def kind: String
  def stringRepr: String

  def idString: String = s"$kind:$stringRepr"
}
object Id {
  final case class Hashed(hash: Hash) extends Id {
    def kind: String = hash.hashAlgorithm.name
    def stringRepr: String = hash.asHexString
  }
  // final case class ByUUID(uuid: UUID) extends Id

  def generic(_kind: String, repr: String): Id =
    new Id {
      def kind: String = _kind
      def stringRepr: String = repr
    }

  import DefaultJsonProtocol._
  implicit def hashedFormat: JsonFormat[Hashed] = jsonFormat1(Hashed.apply _)
  implicit def idFormat: JsonFormat[Id] = new JsonFormat[Id] {
    override def write(obj: Id): JsValue = JsString(obj.idString)
    override def read(json: JsValue): Id = json match {
      case JsString(x) =>
        x.split(":") match {
          case Array("sha-512", data) => Hashed(Hash.fromString(HashAlgorithm.Sha512, data))
        }
      case x => MetadataJsonProtocol.error(s"Cannot read Id from $x")
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

  def entryFormat(knownKinds: Set[MetadataKind]): JsonFormat[MetadataEntry] = {
    case class SimpleEntry(kind: KindHeader)
    case class KindHeader(kind: String, version: Int)

    import DefaultJsonProtocol._
    implicit val headerFormat = jsonFormat2(KindHeader.apply _)

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
    def implFormat(kind: MetadataKind): JsonFormat[Impl[kind.T]] = {
      import kind.jsonFormat
      jsonFormat5(Impl.apply)
    }

    new JsonFormat[MetadataEntry] {
      override def write(obj: MetadataEntry): JsValue =
        obj.asInstanceOf[Impl[obj.T]].toJson(implFormat(obj.kind))
      override def read(json: JsValue): MetadataEntry = {
        val kind = json.asJsObject.field("kind").convertTo[MetadataKind]
        json.convertTo(implFormat(kind))
      }
    }
  }
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
  implicit def envelopeFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[MetadataEnvelope] = {
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

trait MetadataExtractor {
  type EntryT
  def kind: String
  def version: Int
  def metadataKind: MetadataKind.Aux[EntryT]
  def dependsOn: Vector[MetadataKind]

  final def extract(hash: Hash, dependencies: Vector[MetadataEntry], ctx: ExtractionContext): Future[MetadataEntry.Aux[EntryT]] =
    extractEntry(hash, dependencies, ctx)
      .map(value =>
        MetadataEntry(
          Hashed(hash),
          Vector.empty,
          metadataKind,
          CreationInfo(DateTime.now, inferred = true, Extractor(kind, version)),
          value))(ctx.executionContext)

  protected def extractEntry(hash: Hash, dependencies: Vector[MetadataEntry], ctx: ExtractionContext): Future[EntryT]
}

object MetadataJsonProtocol {
  def error(message: String): Nothing = throw DeserializationException(message)

  implicit val dateTimeFormat: JsonFormat[DateTime] = new JsonFormat[DateTime] {
    override def read(json: JsValue): DateTime = json match {
      case JsString(data) => DateTime.fromIsoDateTimeString(data).getOrElse(error(s"Date could not be read [$data]"))
      case x              => MetadataJsonProtocol.error(s"Cannot read DateTime from $x")
    }
    override def write(obj: DateTime): JsValue = JsString(obj.toIsoDateTimeString())
  }
  implicit val byteStringFormat: JsonFormat[ByteString] = new JsonFormat[ByteString] {
    override def read(json: JsValue): ByteString = json match {
      case JsString(data) => ByteString.fromArray(Base64.getDecoder.decode(data))
      case x              => MetadataJsonProtocol.error(s"Cannot read ByteString from $x")
    }
    override def write(obj: ByteString): JsValue = {
      val data = Base64.getEncoder.encodeToString(obj.toArray)
      JsString(data)
    }
  }
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

  def get[T](shortcut: MetadataShortcuts.ShortCut[T]): T = shortcut(this)
}
