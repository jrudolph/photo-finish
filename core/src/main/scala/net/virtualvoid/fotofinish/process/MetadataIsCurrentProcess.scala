package net.virtualvoid.fotofinish.process

import net.virtualvoid.fotofinish.metadata.{ ExtractionContext, Id, MetadataEntry, MetadataEnvelope, MetadataExtractor }
import net.virtualvoid.fotofinish.util.JsonExtra
import spray.json.JsonFormat

import scala.concurrent.{ ExecutionContext, Future }

trait MetadataExtractionScheduler {
  def workHistogram: Future[Map[String, Int]]
}

class MetadataIsCurrentProcess(extractor: MetadataExtractor) extends PerIdProcessWithNoGlobalState {
  type PerKeyState = HashState

  sealed trait DependencyState {
    def exists: Boolean
    def get: MetadataEntry
  }
  case class Missing(kind: String, version: Int) extends DependencyState {
    def exists: Boolean = false
    def get: MetadataEntry = throw new IllegalStateException(s"Metadata for kind $kind was still missing")
  }
  case class Existing(value: MetadataEntry) extends DependencyState {
    def exists: Boolean = true
    def get: MetadataEntry = value
  }

  sealed trait HashState extends Product {
    def handle(entry: MetadataEntry): HashState
  }
  case class CollectingDependencies(dependencyState: Vector[DependencyState]) extends HashState {
    def handle(entry: MetadataEntry): HashState =
      if (entry.kind.kind == extractor.metadataKind.kind)
        if (entry.kind.version == extractor.metadataKind.version) LatestVersion
        else Calculated(entry.kind.version)
      else {
        val newL =
          dependencyState.map {
            case Missing(k, v) if k == entry.kind.kind && v == entry.kind.version => Existing(entry)
            case x => x // FIXME: handle dependency changes
          }

        if (newL forall (_.exists))
          extractor.precondition(entry.target.hash, newL.map(_.get)) match {
            case None        => Ready(newL.map(_.get))
            case Some(cause) => PreConditionNotMet(cause)
          }
        else CollectingDependencies(newL)
      }

    def hasAllDeps: Boolean = dependencyState.forall(_.exists)
  }
  private val Initial = CollectingDependencies(extractor.dependsOn.map(k => Missing(k.kind, k.version)))
  case class Ready(dependencyState: Vector[MetadataEntry]) extends HashState {
    override def handle(entry: MetadataEntry): HashState =
      if (entry.kind.kind == extractor.metadataKind.kind)
        if (entry.kind.version == extractor.metadataKind.version) LatestVersion
        else Calculated(entry.kind.version)
      else this
  }
  case class Scheduled(dependencyState: Vector[MetadataEntry]) extends HashState {
    override def handle(entry: MetadataEntry): HashState =
      if (entry.kind.kind == extractor.metadataKind.kind)
        if (entry.kind.version == extractor.metadataKind.version) LatestVersion
        else Calculated(entry.kind.version)
      else this
  }
  case class Calculated(version: Int) extends HashState {
    override def handle(entry: MetadataEntry): HashState =
      if (entry.kind.kind == extractor.metadataKind.kind)
        if (entry.kind.version == extractor.metadataKind.version) LatestVersion
        else Calculated(version max entry.kind.version)
      else this
    // FIXME: handle dependency changes
  }
  private val LatestVersion = Calculated(extractor.metadataKind.version)
  case class PreConditionNotMet(cause: String) extends HashState {
    override def handle(entry: MetadataEntry): HashState = {
      require(
        !(entry.kind.kind == extractor.metadataKind.kind && entry.kind.version == extractor.metadataKind.version),
        s"Unexpected metadata entry found where previously precondition was not met because of [$cause]")
      this
    }
  }

  type Api = MetadataExtractionScheduler

  override val id: String = s"net.virtualvoid.fotofinish.metadata[${extractor.kind}]"
  def version: Int = 3

  def initialPerKeyState(id: Id): HashState = Initial
  def processIdEvent(id: Id, state: HashState, event: MetadataEnvelope): Effect = Effect.setKeyState(id, state.handle(event.entry))

  def hasWork(id: Id, state: HashState): Boolean = state.isInstanceOf[Ready]
  def createWork(key: Id, state: HashState, context: ExtractionContext): (HashState, Vector[WorkEntry]) =
    state match {
      case Ready(depValues) =>
        (
          Scheduled(depValues),
          Vector(WorkEntry.opaque(() => extractor.extract(key.hash, depValues, context).map(Vector(_))(context.executionContext)))
        )
      case _ => (state, Vector.empty)
    }
  def api(handleWithState: PerIdHandleWithStateFunc[HashState])(implicit ec: ExecutionContext): MetadataExtractionScheduler =
    new MetadataExtractionScheduler {
      def workHistogram: Future[Map[String, Int]] =
        handleWithState.accessAll { states =>
          states
            .toVector // FIXME: could we somehow support these queries in a better way? (e.g. accumulating numbers directly)
            .groupBy(_._2.productPrefix)
            .view.mapValues(_.size).toMap
        }
    }

  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[HashState] = {
    import JsonExtra._
    import spray.json.DefaultJsonProtocol._
    import spray.json._

    implicit def missingFormat: JsonFormat[Missing] = jsonFormat2(Missing)
    implicit def existingFormat: JsonFormat[Existing] = jsonFormat1(Existing)
    implicit def depStateFormat: JsonFormat[DependencyState] = new JsonFormat[DependencyState] {
      def read(json: JsValue): DependencyState =
        json.field("type") match {
          case JsString("Missing")  => json.convertTo[Missing]
          case JsString("Existing") => json.convertTo[Existing]
          case x                    => throw DeserializationException(s"Unexpected type '$x' for DependencyState")
        }
      def write(obj: DependencyState): JsValue = obj match {
        case m: Missing  => m.toJson + ("type" -> JsString("Missing"))
        case e: Existing => e.toJson + ("type" -> JsString("Existing"))
      }
    }

    implicit def collectingFormat: JsonFormat[CollectingDependencies] = jsonFormat1(CollectingDependencies.apply)
    implicit def readyFormat: JsonFormat[Ready] = jsonFormat1(Ready.apply)
    implicit def scheduledFormat: JsonFormat[Scheduled] = jsonFormat1(Scheduled.apply)
    implicit def calculatedFormat: JsonFormat[Calculated] = jsonFormat1(Calculated.apply)
    implicit def preconditionNotMetFormat: JsonFormat[PreConditionNotMet] = jsonFormat1(PreConditionNotMet.apply)
    implicit def hashStateFormat: JsonFormat[HashState] = new JsonFormat[HashState] {
      import net.virtualvoid.fotofinish.util.JsonExtra._
      override def read(json: JsValue): HashState =
        json.field("type") match {
          case JsString("CollectingDependencies") => json.convertTo[CollectingDependencies]
          case JsString("Ready")                  => json.convertTo[Ready]
          case JsString("Scheduled")              => json.convertTo[Scheduled]
          case JsString("Calculated")             => json.convertTo[Calculated]
          case JsString("PreConditionNotMet")     => json.convertTo[PreConditionNotMet]
          case x                                  => throw DeserializationException(s"Unexpected type '$x' for HashState")
        }

      override def write(obj: HashState): JsValue = obj match {
        case c: CollectingDependencies => c.toJson + ("type" -> JsString("CollectingDependencies"))
        case r: Ready                  => r.toJson + ("type" -> JsString("Ready"))
        case s: Scheduled              => s.toJson + ("type" -> JsString("Scheduled"))
        case c: Calculated             => c.toJson + ("type" -> JsString("Calculated"))
        case p: PreConditionNotMet     => p.toJson + ("type" -> JsString("PreConditionNotMet"))
      }
    }
    hashStateFormat
  }

  override def isTransient(state: HashState): Boolean = state.isInstanceOf[Scheduled]
  override def initializeTransientState(key: Id, state: HashState): HashState =
    state match {
      case Scheduled(depValues) => Ready(depValues)
      case x                    => x
    }
}