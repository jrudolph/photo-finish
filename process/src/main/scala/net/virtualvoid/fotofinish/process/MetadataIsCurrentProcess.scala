package net.virtualvoid.fotofinish.process

import net.virtualvoid.fotofinish.Hash
import net.virtualvoid.fotofinish.metadata.{ ExtractionContext, Extractor, Id, MetadataEntry, MetadataEnvelope, MetadataExtractor }
import net.virtualvoid.fotofinish.util.JsonExtra
import spray.json.JsonFormat

import scala.concurrent.{ ExecutionContext, Future }

trait MetadataExtractionScheduler {
  def workHistogram: Future[Map[String, Int]]
}

class MetadataIsCurrentProcess(val extractor: MetadataExtractor) extends PerIdProcess {
  type GlobalState = StateHistogram
  type PerKeyState = HashState

  case class StateHistogram(states: Map[String, Int], scheduled: Set[Id]) {
    def handle(entry: MetadataEntry): Effect = {
      val id = entry.target

      Effect.exists(id) { exists =>
        Effect.flatMapKeyState(id) { oldState =>
          val newState = oldState.handleEntry(entry)
          (newState, Effect.mapGlobalState { global =>
            val nowScheduled = newState.isInstanceOf[Scheduled] // FIXME: use method instead

            val global1 = if (nowScheduled) global.addScheduled(id) else global.removeScheduled(id)
            val global2 =
              if (exists) global1.transition(oldState.productPrefix, newState.productPrefix)
              else global1.inc(newState.productPrefix, 1)
            global2
          })
        }
      }

    }
    def transition(from: String, to: String): StateHistogram =
      if (from != to) inc(from, -1).inc(to, +1)
      else this
    def inc(state: String, by: Int): StateHistogram =
      copy(states = states.updated(state, states.getOrElse(state, 0) + by))
    def addScheduled(id: Id): StateHistogram = copy(scheduled = scheduled + id)
    def removeScheduled(id: Id): StateHistogram = copy(scheduled = scheduled - id)

    def schedule(id: Id): Effect =
      Effect.flatMapGlobalState { s =>
        (s.addScheduled(id).transition("Ready", "Scheduled"), Effect.mapKeyState(id) {
          case Ready(deps) => Scheduled(deps)
          case _           => throw new IllegalStateException
        })
      }
  }

  def initialGlobalState: StateHistogram = StateHistogram(Map.empty, Set.empty)

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

  val ExtractorId = extractor.kind
  val ExtractorVersion = extractor.version
  sealed trait HashState extends Product {
    def handle(entry: MetadataEntry): HashState

    def dependencyState: Vector[DependencyState]

    def handleEntry(entry: MetadataEntry): HashState =
      if (entry.kind.kind == extractor.metadataKind.kind)
        if (dependencyState.forall(_.exists))
          handleUpgrade(entry.cast(extractor.metadataKind), dependencyState.map(_.get))
        else
          CollectingDependencies(dependencyState, Some(entry.cast(extractor.metadataKind)))
      else
        handle(entry)

    def handleUpgrade(entry: MetadataEntry.Aux[extractor.EntryT], deps: Vector[MetadataEntry]): HashState =
      entry.creation.creator match {
        case Extractor(ExtractorId, ExtractorVersion) => Calculated(ExtractorVersion, deps)
        case _ =>
          extractor.upgradeExisting(entry, deps) match {
            case MetadataExtractor.Keep => Calculated(ExtractorVersion, deps)
            //case MetadataExtractor.PublishUpgraded(newEntry) => ??? // FIXME: create new state that can create a simple workitem to publish new entry
            case MetadataExtractor.RerunExtractor =>
              handleReady(entry.target.hash, deps)
          }
      }
    def handleReady(hash: Hash, deps: Vector[MetadataEntry]): HashState =
      extractor.precondition(hash, deps) match {
        case None        => Ready(deps)
        case Some(cause) => PreConditionNotMet(cause, deps)
      }
  }
  case class CollectingDependencies(dependencyState: Vector[DependencyState], upgradeExisting: Option[MetadataEntry.Aux[extractor.EntryT]]) extends HashState {
    def handle(entry: MetadataEntry): HashState = {
      val newL =
        dependencyState.map {
          case Missing(k, v) if k == entry.kind.kind && v == entry.kind.version => Existing(entry)
          case x => x // FIXME: handle dependency changes
        }

      if (newL forall (_.exists)) {
        val deps = newL.map(_.get)
        upgradeExisting match {
          case Some(existing) => handleUpgrade(existing, deps)
          case None           => handleReady(entry.target.hash, deps)
        }
      } else CollectingDependencies(newL, upgradeExisting)
    }

    override def productPrefix: String = if (upgradeExisting.isDefined) "CollectingDependenciesForUpgrade" else "CollectingDependencies"
  }
  private val Initial = CollectingDependencies(extractor.dependsOn.map(k => Missing(k.kind, k.version)), None)
  case class Ready(dependencies: Vector[MetadataEntry]) extends HashState {
    override def handle(entry: MetadataEntry): HashState = {
      val newDeps = dependencies.map { existing =>
        // FIXME: is take latest the only reasonable strategy?
        if (existing.kind.kind == entry.kind.kind && existing.creation.created < entry.creation.created) entry
        else existing
      }
      handleReady(entry.target.hash, newDeps)
    }

    override def dependencyState: Vector[DependencyState] = dependencies.map(Existing)
  }
  case class Scheduled(dependencies: Vector[MetadataEntry]) extends HashState {
    override def handle(entry: MetadataEntry): HashState = this
    override def dependencyState: Vector[DependencyState] = dependencies.map(Existing)
  }
  case class Calculated(extractorVersion: Int, dependencies: Vector[MetadataEntry]) extends HashState {
    override def handle(entry: MetadataEntry): HashState = this
    // FIXME: handle dependency changes, hard to to? We don't keep track persistently what the dependencies were

    override def dependencyState: Vector[DependencyState] = dependencies.map(Existing)
  }
  case class PreConditionNotMet(cause: String, dependencies: Vector[MetadataEntry]) extends HashState {
    override def handle(entry: MetadataEntry): HashState = {
      require(
        !(entry.kind.kind == extractor.metadataKind.kind && entry.kind.version == extractor.metadataKind.version),
        s"Unexpected metadata entry found where previously precondition was not met because of [$cause]")

      val newDeps = dependencies.map { existing =>
        // FIXME: is take latest the only reasonable strategy?
        if (existing.kind.kind == entry.kind.kind && existing.creation.created < entry.creation.created) entry
        else existing
      }
      if (newDeps != dependencies)
        handleReady(entry.target.hash, newDeps)
      else
        this
    }
    override def dependencyState: Vector[DependencyState] = dependencies.map(Existing)
  }

  type Api = MetadataExtractionScheduler

  override val id: String = s"net.virtualvoid.fotofinish.metadata[${extractor.kind}]"
  def version: Int = 7

  def initialPerKeyState(id: Id): HashState = Initial
  def processIdEvent(id: Id, event: MetadataEnvelope): Effect =
    Effect.accessFlatMapGlobalState(_.handle(event.entry))

  override def hasWork(id: Id, state: HashState): Boolean = state.isInstanceOf[Ready]
  override def createWork(key: Id, state: HashState, context: ExtractionContext): (Effect, Vector[WorkEntry]) =
    state match {
      case Ready(depValues) =>
        (
          Effect.accessFlatMapGlobalState(_.schedule(key)),
          Vector(WorkEntry.opaque(() => extractor.extract(key.hash, depValues, context).map(Vector(_))(context.executionContext)))
        )
      case _ => (Effect.Empty, Vector.empty)
    }
  def api(handleWithState: AccessStateFunc)(implicit ec: ExecutionContext): MetadataExtractionScheduler =
    new MetadataExtractionScheduler {
      def workHistogram: Future[Map[String, Int]] = handleWithState.accessGlobal(_.states)
    }

  import spray.json.DefaultJsonProtocol._
  def globalStateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[StateHistogram] = jsonFormat2(StateHistogram.apply _)
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

    implicit def collectingFormat: JsonFormat[CollectingDependencies] = jsonFormat2(CollectingDependencies.apply)
    implicit def readyFormat: JsonFormat[Ready] = jsonFormat1(Ready.apply)
    implicit def scheduledFormat: JsonFormat[Scheduled] = jsonFormat1(Scheduled.apply)
    implicit def calculatedFormat: JsonFormat[Calculated] = jsonFormat2(Calculated.apply)
    implicit def preconditionNotMetFormat: JsonFormat[PreConditionNotMet] = jsonFormat2(PreConditionNotMet.apply)
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

  override def initializeSnapshot: Effect =
    Effect.flatMapGlobalState { global =>
      val sched = global.scheduled
      val es = sched.map(k => Effect.mapKeyState(k) {
        case Scheduled(deps) => Ready(deps)
        case _               => throw new IllegalStateException
      })

      (global.copy(scheduled = Set.empty), Effect.and(es.toSeq))
    }
}