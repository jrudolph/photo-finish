package net.virtualvoid.fotofinish

import java.io.{ File, FileInputStream, FileOutputStream }
import java.util.zip.{ GZIPInputStream, GZIPOutputStream }

import akka.actor.ActorSystem
import akka.http.scaladsl.model.DateTime
import akka.pattern.after
import akka.stream._
import akka.stream.scaladsl.{ BroadcastHub, Compression, FileIO, Flow, Framing, Keep, MergeHub, Sink, Source }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.util.ByteString
import net.virtualvoid.fotofinish.MetadataProcess.{ AllObjectsReplayed, SideEffect, StreamEntry }
import net.virtualvoid.fotofinish.metadata.Id.Hashed
import net.virtualvoid.fotofinish.metadata._
import net.virtualvoid.fotofinish.util.JsonExtra
import spray.json.JsonFormat

import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.immutable.{ TreeMap, TreeSet }
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.language.implicitConversions
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

trait HandleWithStateFunc[S] {
  def apply[T](f: S => (S, Vector[SideEffect], T)): Future[T]
  def access[T](f: S => T): Future[T] =
    apply { state =>
      (state, Vector.empty, f(state))
    }
}

trait MetadataProcess {
  type S
  type Api

  def id: String = getClass.getName
  def version: Int

  def initialState: S
  def processEvent(state: S, event: MetadataEnvelope): S
  def sideEffects(state: S, context: ExtractionContext): (S, Vector[SideEffect])
  def api(handleWithState: HandleWithStateFunc[S])(implicit ec: ExecutionContext): Api

  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[S]
  /** Allows to prepare state loaded from snapshot */
  def initializeStateSnapshot(state: S): S = state
}

object MetadataProcess {
  // TODO: make those work items a proper trait that includes metadata about a work item
  // like expected workload in terms of CPU, IO, etc.
  type SideEffect = () => Future[Vector[MetadataEntry]]

  sealed trait StreamEntry
  final case class Metadata(entry: MetadataEnvelope) extends StreamEntry
  case object AllObjectsReplayed extends StreamEntry
  case object MakeSnapshot extends StreamEntry
  case object ShuttingDown extends StreamEntry
  final case class Execute[S, T](f: S => (S, Vector[SideEffect], T), promise: Promise[T]) extends StreamEntry

  def asSource(p: MetadataProcess, manager: RepositoryManager, journal: Journal, extractionEC: ExecutionContext)(implicit ec: ExecutionContext): Source[SideEffect, p.Api] = {
    val injectApi: Source[StreamEntry, p.Api] =
      Source.queue[StreamEntry](5000, OverflowStrategy.dropNew) // FIXME: will this be enough for mass injections?
        .mapMaterializedValue { queue =>
          p.api(new HandleWithStateFunc[p.S] {
            def apply[T](f: p.S => (p.S, Vector[SideEffect], T)): Future[T] = {
              val promise = Promise[T]
              queue.offer(Execute(f, promise))
                .onComplete {
                  case Success(QueueOfferResult.Enqueued) =>
                  case x                                  => println(s"Injecting entries failed because of $x")
                }
              promise.future
            }
          })
        }

    val snapshot = deserializeState(p, manager).getOrElse(Snapshot(p.id, p.version, -1L, p.initialState))
    val extractionContext = new ExtractionContext {
      def executionContext: ExecutionContext = extractionEC
      def accessData[T](hash: Hash)(f: File => Future[T]): Future[T] =
        // FIXME: easy for now as we expect all hashes to be available as files
        f(manager.config.fileInfoOf(hash).repoFile)
    }

    val processFlow =
      Flow[StreamEntry]
        .merge(Source.tick(manager.config.snapshotInterval, manager.config.snapshotInterval, MakeSnapshot))
        .mergeMat(injectApi)(Keep.right)
        .statefulMapConcat[SideEffect] { () =>
          case class Effect(sideEffects: immutable.Iterable[SideEffect], nextHandler: Handler, newSeqNr: Long, newState: p.S, runStreamEntry: StreamEntry = null) {
            def schedule(newSideEffects: immutable.Iterable[SideEffect]): Effect = copy(sideEffects ++ newSideEffects)
            def changeState(state: p.S): Effect = copy(newState = state)
            def changeSeqNr(seqNr: Long): Effect = copy(newSeqNr = seqNr)
            def changeHandler(handler: Handler): Effect = copy(nextHandler = handler)
            def andRun(entry: StreamEntry): Effect = copy(runStreamEntry = entry)
          }
          val SameHandler: Handler = _ => ???
          object Effect {
            def empty: Effect = Effect(Vector.empty, SameHandler, -2, null.asInstanceOf[p.S])
          }
          implicit def effectMethods(e: Effect.type): Effect = Effect.empty
          implicit def changeHandler(h: Handler): Effect = Effect.changeHandler(h)
          abstract class Handler {
            def apply(e: StreamEntry): Effect
          }
          var _curSeqNr: Long = -1
          var _curState: p.S = null.asInstanceOf[p.S]
          def currentSeqNr: Long = _curSeqNr
          def currentState: p.S = _curState

          def replaying(waitingExecutions: Vector[Execute[p.S, _]]): Handler = {
            case Metadata(e) =>
              if (e.seqNr == currentSeqNr + 1) {
                val newState = p.processEvent(currentState, e)
                Effect
                  .changeSeqNr(e.seqNr)
                  .changeState(newState)
              } else if (e.seqNr < currentSeqNr + 1)
                throw new IllegalStateException(s"Got unexpected duplicate seqNr ${e.seqNr} after last $currentSeqNr, ignoring...")
              else {
                println(s"Got unexpected gap in seqNr ${e.seqNr} after last $currentSeqNr, ignoring...")
                Effect.changeSeqNr(e.seqNr)
              }

            case AllObjectsReplayed =>
              println(s"[${p.id}] finished replaying")

              val (newState0, sideEffects0) = p.sideEffects(currentState, extractionContext)

              // run all waiting executions
              val (newState, sideEffects) = waitingExecutions.foldLeft((newState0, sideEffects0)) { (cur, next) =>
                val (lastState, lastEffects) = cur
                val (nextEffects, nextState) = execute(next, lastState)
                (nextState, lastEffects ++ nextEffects)
              }

              Effect
                .changeHandler(ignoreDuplicateSeqNrs)
                .changeState(newState)
                .schedule(sideEffects)

            case e: Execute[p.S, t] @unchecked => replaying(waitingExecutions :+ e)

            case MakeSnapshot                  => doSnapshot()
            case ShuttingDown                  => shutdown()
          }
          def ignoreDuplicateSeqNrs: Handler = {
            case Metadata(e) if e.seqNr <= currentSeqNr =>
              println(s"[${p.id}] at $currentSeqNr got ${e.seqNr}. Ignoring")
              Effect.empty
            case m: Metadata => Effect.changeHandler(liveEvents).andRun(m)

            case e: Execute[p.S, t] @unchecked =>
              val (sideEffects, newState) = execute(e, currentState)
              Effect
                .changeState(newState)
                .schedule(sideEffects)

            case MakeSnapshot => doSnapshot()
            case ShuttingDown => shutdown()
          }
          def liveEvents: Handler = {
            case Metadata(e) =>
              if (e.seqNr == currentSeqNr + 1) {
                val newState0 = p.processEvent(currentState, e)
                val (newState1, sideEffects) = p.sideEffects(newState0, extractionContext)
                Effect
                  .changeSeqNr(e.seqNr)
                  .changeState(newState1)
                  .schedule(sideEffects)
              } else if (e.seqNr < currentSeqNr + 1)
                throw new IllegalStateException(s"Got unexpected duplicate seqNr ${e.seqNr} after last $currentSeqNr, ignoring...")
              else {
                println(s"Got unexpected gap in seqNr ${e.seqNr} after last $currentSeqNr, ignoring...")
                Effect
                  .changeSeqNr(e.seqNr)
              }
            case e: Execute[p.S, t] @unchecked =>
              val (sideEffects, newState) = execute(e, currentState)
              Effect
                .changeState(newState)
                .schedule(sideEffects)

            case MakeSnapshot       => doSnapshot()
            case ShuttingDown       => shutdown()

            case AllObjectsReplayed => throw new IllegalStateException("Unexpected AllObjectsReplayed in state liveEvents")
          }
          def doSnapshot(): Effect = {
            saveSnapshot()
            Effect.empty
          }
          def shutdown(): Effect = {
            saveSnapshot()
            closing
          }
          def saveSnapshot(): Unit = serializeState(p, manager)(Snapshot(p.id, p.version, currentSeqNr, currentState))

          // FIXME: try to use Effect here as well (complicated because then we need to be able to stack effects
          def execute[T](e: Execute[p.S, T], currentState: p.S): (immutable.Iterable[SideEffect], p.S) =
            Try(e.f(currentState)) match {
              case Success((newState, sideEffects, res)) =>
                e.promise.trySuccess(res)
                (sideEffects, newState)
              case Failure(ex) =>
                e.promise.tryFailure(ex)
                (Nil, currentState)
            }

          lazy val closing: Handler = _ => changeHandler(SameHandler)

          println(s"[${p.id}] initialized process at seqNr [${snapshot.currentSeqNr}]")
          _curSeqNr = snapshot.currentSeqNr
          _curState = snapshot.state
          var curHandler: Handler = replaying(Vector.empty)

          e => {
            @tailrec def run(sideEffects: Vector[SideEffect]): Vector[SideEffect] = {
              val Effect(els, newHandler, newSeqNr, newState, runStreamEntry) = curHandler(e)
              if (newHandler ne SameHandler) curHandler = newHandler
              if (newSeqNr != -2) _curSeqNr = newSeqNr
              if (newState != null) _curState = newState

              val newSideEffects = sideEffects ++ els
              if (runStreamEntry ne null) run(newSideEffects)
              else newSideEffects
            }
            run(Vector.empty)
          }
        }
        .recoverWithRetries(1, {
          case ex =>
            println(s"Process for [${p.id}] failed with [${ex.getMessage}]. Aborting the process.")
            ex.printStackTrace()
            Source.empty
        })

    journal.source(snapshot.currentSeqNr + 1)
      .map {
        case m @ Metadata(entry) =>
          if ((entry.seqNr % 100) == 0) println(s"[${p.id}] at [${entry.seqNr}]")

          m
        case e =>
          println(s"[${p.id}] at [$e]")
          e

      }
      .viaMat(processFlow)(Keep.right)
  }

  private case class Snapshot[S](processId: String, processVersion: Int, currentSeqNr: Long, state: S)
  import spray.json.DefaultJsonProtocol._
  import spray.json._
  private implicit def snapshotFormat[S: JsonFormat]: JsonFormat[Snapshot[S]] = jsonFormat4(Snapshot.apply[S])
  private def processSnapshotFile(p: MetadataProcess, manager: RepositoryManager): File =
    new File(manager.config.metadataDir, s"${p.id.replaceAll("""[\[\]]""", "_")}.snapshot.json.gz")

  private def serializeState(p: MetadataProcess, manager: RepositoryManager)(snapshot: Snapshot[p.S]): Unit = {
    val file = processSnapshotFile(p, manager)
    val os = new GZIPOutputStream(new FileOutputStream(file))
    try {
      implicit val sFormat: JsonFormat[p.S] = p.stateFormat(manager.config.entryFormat)
      os.write(snapshot.toJson.compactPrint.getBytes("utf8"))
    } finally os.close()
  }
  private def deserializeState(p: MetadataProcess, manager: RepositoryManager): Option[Snapshot[p.S]] = {
    val file = processSnapshotFile(p, manager)
    if (file.exists) {
      val fis = new FileInputStream(file)
      try {
        val is = new GZIPInputStream(fis)
        implicit val sFormat: JsonFormat[p.S] = p.stateFormat(manager.config.entryFormat)
        Try(io.Source.fromInputStream(is).mkString.parseJson.convertTo[Snapshot[p.S]])
          .map { s =>
            require(s.processId == p.id, s"Unexpected process ID in snapshot [${s.processId}]. Expected [${p.id}].")
            require(s.processVersion == p.version, s"Wrong version in snapshot [${s.processVersion}]. Expected [${p.version}].")

            Some(s.copy(state = p.initializeStateSnapshot(s.state)))
          }
          .recover {
            case NonFatal(ex) =>
              println(s"Reading snapshot failed because of ${ex.getMessage}. Discarding snapshot.")
              ex.printStackTrace()
              None
          }
          .get
      } finally fis.close()
    } else
      None
  }

  trait Journal {
    def newEntrySink: Sink[MetadataEntry, Any]
    def source(fromSeqNr: Long): Source[StreamEntry, Any]
    def shutdown(): Unit
  }

  /**
   * A flow that produces existing entries and consumes new events to be written to the journal.
   * The flow can be reused.
   */
  def journal(manager: RepositoryManager)(implicit system: ActorSystem): Journal = {
    import system.dispatcher

    val killSwitch = KillSwitches.shared("kill-journal")
    val seqNrFile = new File(manager.config.metadataDir, "metadata.seqnr.txt")
    def readSeqNr(): Long = Try(scala.io.Source.fromFile(seqNrFile).mkString.toLong).getOrElse(-1)
    def writeSeqNr(last: Long): Unit = {
      // FIXME: what are the performance implications of closing every time?
      val fos = new FileOutputStream(seqNrFile)
      fos.write(last.toString.getBytes())
      fos.close()
    }

    val liveJournalFlow: Flow[MetadataEntry, MetadataEnvelope, Any] =
      Flow[MetadataEntry]
        .statefulMapConcat[MetadataEnvelope] { () =>
          var lastSeqNo: Long = readSeqNr()

          entry =>
            Try {
              val thisSeqNr = lastSeqNo + 1

              val entryWithSeqNr = MetadataEnvelope(thisSeqNr, entry)
              writeJournalEntry(manager, entryWithSeqNr)
              // update seqnr last
              writeSeqNr(thisSeqNr)
              lastSeqNo += 1

              entryWithSeqNr :: Nil
            }.recover {
              case ex =>
                println(s"[journal] processing new entry [$entry] failed with [${ex.getMessage}], dropping")
                ex.printStackTrace()
                Nil
            }.get
        }

    def readAllEntries(): Future[Source[MetadataEnvelope, Any]] =
      // add some delay which should help to make sure that live stream is running and connected when we start reading the
      // file
      after(100.millis, system.scheduler) {
        Future.successful {
          if (manager.config.allMetadataFile.exists())
            FileIO.fromPath(manager.config.allMetadataFile.toPath)
              .via(Compression.gunzip())
              .via(Framing.delimiter(ByteString("\n"), 1000000))
              .map(_.utf8String)
              .mapConcat(readJournalEntry(manager, _).toVector)
          else
            Source.empty
        }
      }

    // A persisting source that replays all existing journal entries on and on as long as something listens to it.
    // The idea is similar to IP multicast that multiple subscribers can attach to the already running program and
    // the overhead for providing the data is only paid once for all subscribers.
    // Another analogy would be a news ticker on a news channel where casual watchers can come and go at any point in
    // time and would eventually get all the information as everyone else (without requiring a dedicated TV for any watcher).
    val existingEntryWheel: Source[StreamEntry, Any] =
      Source
        .fromGraph(new RepeatSource(
          Source.lazyFutureSource(readAllEntries).map[StreamEntry](Metadata)
            .concat(
              Source.single(AllObjectsReplayed)
            )
        ))
        .runWith(BroadcastHub.sink(1024))

    // Attaches to the wheel but only returns the journal entries between the given seqNr and AllObjectsReplayed.
    def existingEntriesStartingWith(fromSeqNr: Long): Source[StreamEntry, Any] =
      existingEntryWheel
        .via(new TakeFromWheel(fromSeqNr))

    val (liveSink, liveSource) =
      MergeHub.source[MetadataEntry]
        .via(liveJournalFlow)
        .via(killSwitch.flow)
        .toMat(BroadcastHub.sink[MetadataEnvelope](2048))(Keep.both)
        .run()

    new Journal {
      override def newEntrySink: Sink[MetadataEntry, Any] = liveSink
      override def source(fromSeqNr: Long): Source[StreamEntry, Any] =
        existingEntriesStartingWith(fromSeqNr)
          .concat(liveSource.map(Metadata))
          .concat(Source.single(ShuttingDown))
      override def shutdown(): Unit = killSwitch.shutdown()
    }
  }

  private def readJournalEntry(manager: RepositoryManager, entry: String): Option[MetadataEnvelope] = {
    import manager.config.entryFormat
    try Some(entry.parseJson.convertTo[MetadataEnvelope])
    catch {
      case NonFatal(ex) =>
        println(s"Couldn't read [$entry] because of ${ex.getMessage}")
        ex.printStackTrace()
        None
    }
  }

  private def writeJournalEntry(manager: RepositoryManager, envelope: MetadataEnvelope): Unit = {
    val fos = new FileOutputStream(manager.config.allMetadataFile, true)
    val out = new GZIPOutputStream(fos)
    import manager.config.entryFormat
    out.write(envelope.toJson.compactPrint.getBytes("utf8"))
    out.write('\n')
    out.close()
    fos.close()
  }
}

object GetAllObjectsProcess extends MetadataProcess {
  case class State(knownHashes: Set[Hash])
  type S = State
  type Api = () => Future[Set[Hash]]

  def version: Int = 1

  def initialState: State = State(Set.empty)
  def processEvent(state: State, event: MetadataEnvelope): State =
    state.copy(knownHashes = state.knownHashes + event.entry.target.hash)
  def sideEffects(state: State, context: ExtractionContext): (State, Vector[SideEffect]) = (state, Vector.empty)

  def api(handleWithState: HandleWithStateFunc[S])(implicit ec: ExecutionContext): Api =
    () => handleWithState { state =>
      (state, Vector.empty, state.knownHashes)
    }

  import spray.json.DefaultJsonProtocol._
  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[State] = jsonFormat1(State.apply)
}

object IngestionController extends MetadataProcess {
  case class State(datas: Map[Hash, Vector[IngestionData]]) {
    def add(hash: Hash, data: IngestionData): State =
      copy(datas = datas + (hash -> (datas.getOrElse(hash, Vector.empty) :+ data)))
  }
  type S = State
  type Api = FileInfo => Unit

  override def version: Int = 1
  override def initialState: State = State(Map.empty)

  override def processEvent(state: State, event: MetadataEnvelope): State = event.entry match {
    case entry if entry.kind == IngestionData => state.add(entry.target.hash, entry.value.asInstanceOf[IngestionData])
    case _                                    => state
  }
  override def sideEffects(state: State, context: ExtractionContext): (State, Vector[SideEffect]) = (state, Vector.empty)

  def api(handleWithState: HandleWithStateFunc[S])(implicit ec: ExecutionContext): FileInfo => Unit = { fi =>
    def matches(data: IngestionData): Boolean =
      fi.originalFile.exists(f => f.getName == data.originalFileName && f.getParent == data.originalFilePath)

    handleWithState { state =>
      //println(s"Checking if $fi needs ingesting...")

      val newEntries =
        if (!state.datas.get(fi.hash).exists(_.exists(matches))) {
          println(s"Injecting [$fi]")
          //IngestionDataExtractor.extractMetadata(fi).toOption.toVector
          Vector(MetadataEntry(
            Id.Hashed(fi.hash),
            Vector.empty,
            IngestionData,
            CreationInfo(DateTime.now, false, Ingestion),
            IngestionData.fromFileInfo(fi)
          ))
        } else {
          //println(s"Did not ingest $fi because there already was an entry")
          Vector.empty
        }

      (state, Vector(() => Future.successful(newEntries)), ())
    }
  }
  import spray.json._
  import DefaultJsonProtocol._
  implicit val ingestionDataFormat = IngestionData.jsonFormat
  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[State] = jsonFormat1(State.apply)
}

class MetadataIsCurrentProcess(extractor: MetadataExtractor) extends MetadataProcess {
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

  sealed trait HashState {
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
        CollectingDependencies(newL)
      }

    def hasAllDeps: Boolean = dependencyState.forall(_.exists)
  }
  private val Initial = CollectingDependencies(extractor.dependsOn.map(k => Missing(k.kind, k.version)))
  case class Scheduled(dependencyState: Vector[DependencyState]) extends HashState {
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

  case class State(objectStates: Map[Hash, HashState]) {
    def get(hash: Hash): Option[HashState] = objectStates.get(hash)
    def set(hash: Hash, newState: HashState): State = mutateStates(_.updated(hash, newState))

    def mutateStates(f: Map[Hash, HashState] => Map[Hash, HashState]): State =
      copy(objectStates = f(objectStates))

    def handle(entry: MetadataEntry): State = {
      val newVal =
        objectStates.get(entry.target.hash) match {
          case Some(s) => s.handle(entry)
          case None    => Initial.handle(entry)
        }
      set(entry.target.hash, newVal)
    }
  }

  type S = State
  type Api = Unit

  override val id: String = s"net.virtualvoid.fotofinish.metadata[${extractor.kind}]"
  def version: Int = 1

  def initialState: S = State(Map.empty)

  def processEvent(state: S, event: MetadataEnvelope): S = state.handle(event.entry)

  def sideEffects(state: S, context: ExtractionContext): (S, Vector[SideEffect]) = {
    val (stateChanges: Vector[State => State], sideeffects: Vector[SideEffect]) = state.objectStates.collect {
      case (hash, c @ CollectingDependencies(depStates)) if c.hasAllDeps =>
        val depValues = depStates.map(_.get)

        extractor.precondition(hash, depValues, context) match {
          case None =>
            // precondition met
            (
              (_: State).set(hash, Scheduled(depStates)),
              (() => extractor.extract(hash, depValues, context).map(Vector(_))(context.executionContext)): SideEffect
            )
          case Some(cause) =>
            (
              (_: State).set(hash, PreConditionNotMet(cause)),
              (() => Future.successful(Vector.empty)) // FIXME: is there a better way that doesn't involve scheduling a useless process?
            )
        }

    }.toVector.unzip
    if (stateChanges.isEmpty) (state, Vector.empty)
    else (stateChanges.foldLeft(state)((s, f) => f(s)), sideeffects)
  }

  def api(handleWithState: HandleWithStateFunc[State])(implicit ec: ExecutionContext): Unit = ()

  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[State] = {
    import spray.json.DefaultJsonProtocol._
    import spray.json._
    import JsonExtra._

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
    implicit def scheduledFormat: JsonFormat[Scheduled] = jsonFormat1(Scheduled.apply)
    implicit def calculatedFormat: JsonFormat[Calculated] = jsonFormat1(Calculated.apply)
    implicit def preconditionNotMetFormat: JsonFormat[PreConditionNotMet] = jsonFormat1(PreConditionNotMet.apply)
    implicit def hashStateFormat: JsonFormat[HashState] = new JsonFormat[HashState] {
      import net.virtualvoid.fotofinish.util.JsonExtra._
      override def read(json: JsValue): HashState =
        json.field("type") match {
          case JsString("CollectingDependencies") => json.convertTo[CollectingDependencies]
          case JsString("Scheduled")              => json.convertTo[Scheduled]
          case JsString("Calculated")             => json.convertTo[Calculated]
          case JsString("PreConditionNotMet")     => json.convertTo[PreConditionNotMet]
          case x                                  => throw DeserializationException(s"Unexpected type '$x' for HashState")
        }

      override def write(obj: HashState): JsValue = obj match {
        case c: CollectingDependencies => c.toJson + ("type" -> JsString("CollectingDependencies"))
        case s: Scheduled              => s.toJson + ("type" -> JsString("Scheduled"))
        case c: Calculated             => c.toJson + ("type" -> JsString("Calculated"))
        case p: PreConditionNotMet     => p.toJson + ("type" -> JsString("PreConditionNotMet"))
      }
    }
    jsonFormat1(State.apply)
  }

  override def initializeStateSnapshot(state: State): State =
    state.mutateStates(_.mapValues {
      case Scheduled(depStates) => CollectingDependencies(depStates) // throw away markers that a calculation is already in process
      case x                    => x
    })
}

trait MetadataApi {
  def metadataFor(id: Id): Future[Metadata]
  def knownObjects(): Future[TreeSet[Id]]
}

object PerObjectMetadataCollector extends MetadataProcess {
  case class State(
      metadata: TreeMap[Hash, Metadata]
  ) {
    def handle(entry: MetadataEntry): State = metadata.get(entry.target.hash) match {
      case Some(Metadata(existing)) =>
        State(metadata + (entry.target.hash -> Metadata(existing :+ entry)))
      case None => State(metadata + (entry.target.hash -> Metadata(Vector(entry))))
    }
  }

  override type S = State
  override type Api = MetadataApi

  override def version: Int = 1

  override def initialState: State = State(TreeMap.empty)
  override def processEvent(state: State, event: MetadataEnvelope): State = state.handle(event.entry)
  override def sideEffects(state: State, context: ExtractionContext): (State, Vector[SideEffect]) = (state, Vector.empty)
  override def api(handleWithState: HandleWithStateFunc[State])(implicit ec: ExecutionContext): MetadataApi =
    new MetadataApi {
      def metadataFor(id: Id): Future[Metadata] =
        handleWithState.access { state => state.metadata.getOrElse(id.hash, Metadata(Vector.empty)) }
      override def knownObjects(): Future[TreeSet[Id]] =
        handleWithState.access { state => TreeSet(state.metadata.keys.map(Hashed(_): Id).toVector: _*) }
    }

  import spray.json.DefaultJsonProtocol._
  implicit def treeMapFormat[K: Ordering: JsonFormat, V: JsonFormat]: JsonFormat[TreeMap[K, V]] =
    JsonExtra.deriveFormatFrom[Map[K, V]](_.toMap, m => TreeMap(m.toSeq: _*))

  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[State] = {
    implicit def metadataFormat: JsonFormat[Metadata] = jsonFormat1(Metadata.apply _)

    jsonFormat1(State.apply _)
  }
}

/**
 * A source that infinitely repeats the given source (by rematerializing it when the previous
 * instance was exhausted.
 *
 * Note that the elements can differ between rematerializations depending on the given source.
 */
class RepeatSource[T](source: Source[T, Any]) extends GraphStage[SourceShape[T]] {
  val out = Outlet[T]("RepeatSource.out")
  val shape: SourceShape[T] = SourceShape(out)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    val waitingForInitialPull = new OutHandler {
      override def onPull(): Unit = {
        val in = new SubSinkInlet[T]("RepeatSource.in")
        val handler = supplying(in)
        in.setHandler(handler)
        setHandler(out, handler)
        in.pull()
        source.runWith(in.sink)(subFusingMaterializer)
      }
    }
    setHandler(out, waitingForInitialPull)

    def supplying(in: SubSinkInlet[T]): OutHandler with InHandler = new OutHandler with InHandler {
      override def onPull(): Unit = if (!in.hasBeenPulled) in.pull()
      override def onDownstreamFinish(cause: Throwable): Unit = {
        in.cancel(cause)
        super.onDownstreamFinish(cause)
      }

      override def onPush(): Unit = push(out, in.grab())
      override def onUpstreamFinish(): Unit = {
        setHandler(out, waitingForInitialPull)
        if (isAvailable(out)) waitingForInitialPull.onPull()
      }
      // just propagate failure
      // override def onUpstreamFailure(ex: Throwable): Unit = super.onUpstreamFailure(ex)
    }
  }
}

/** A stage that ignores element from the journal wheel until it finds the first matching  */
class TakeFromWheel(firstSeqNr: Long) extends GraphStage[FlowShape[StreamEntry, StreamEntry]] {
  val in = Inlet[StreamEntry]("TakeFromWheel.in")
  val out = Outlet[StreamEntry]("TakeFromWheel.out")
  val shape: FlowShape[StreamEntry, StreamEntry] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with OutHandler {
    import MetadataProcess.Metadata

    def onPull(): Unit = pull(in)

    val waitForStart = new InHandler {
      var seenSmaller: Boolean = false
      override def onPush(): Unit =
        grab(in) match {
          case e @ Metadata(entry) =>
            seenSmaller ||= entry.seqNr < firstSeqNr

            if (entry.seqNr == firstSeqNr) {
              push(out, e)
              setHandler(in, transferring)
            } else if (seenSmaller && entry.seqNr > firstSeqNr)
              throw new IllegalStateException(s"Gap in journal before ${entry.seqNr}")
            // else if (!seenSmaller && entry.seqNr > firstSeqNr) // need to wrap first
            // else if (entry.seqNr < firstSeqNr) // seenSmaller was set, continue
            else pull(in)

          case AllObjectsReplayed =>
            if (seenSmaller) { // seq nr not yet available
              push(out, AllObjectsReplayed)
              completeStage()
            } else {
              seenSmaller = true // when wrapping, we treat the end signal as smallest seqNr
              pull(in)
            }
          case x => throw new IllegalStateException(s"Unexpected element $x")
        }
    }
    val transferring = new InHandler {
      override def onPush(): Unit = {
        val el = grab(in)
        push(out, el)
        if (el == AllObjectsReplayed) completeStage()
      }
    }

    setHandler(out, this)
    setHandler(in, waitForStart)
  }
}