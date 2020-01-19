package net.virtualvoid.fotofinish
package process

import java.io.{ File, FileOutputStream }
import java.nio.file.{ Files, StandardCopyOption }
import java.util.zip.GZIPOutputStream

import akka.actor.ActorSystem
import akka.http.scaladsl.model.DateTime
import akka.stream._
import akka.stream.scaladsl.{ Compression, FileIO, Flow, Framing, Keep, MergeHub, Sink, Source }
import akka.util.ByteString
import net.virtualvoid.fotofinish.metadata.Id.Hashed
import net.virtualvoid.fotofinish.metadata._
import net.virtualvoid.fotofinish.util.{ JsonExtra, StatefulDetachedFlow }
import spray.json.JsonFormat

import scala.collection.immutable.{ TreeMap, TreeSet }
import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

trait HandleWithStateFunc[S] {
  def apply[T](f: S => (S, Vector[WorkEntry], T)): Future[T]
  def access[T](f: S => T): Future[T] =
    apply { state =>
      (state, Vector.empty, f(state))
    }

  def handleStream: Sink[S => (S, Vector[WorkEntry]), Any]
}

trait MetadataProcess {
  type S
  type Api

  def id: String = getClass.getName
  def version: Int

  def initialState: S
  def processEvent(state: S, event: MetadataEnvelope): S
  def createWork(state: S, context: ExtractionContext): (S, Vector[WorkEntry])
  def api(handleWithState: HandleWithStateFunc[S])(implicit ec: ExecutionContext): Api

  /** Allows to prepare state loaded from snapshot */
  def initializeStateSnapshot(state: S): S = state

  type StateEntryT
  def stateAsEntries(state: S): Iterator[StateEntryT]
  def entriesAsState(entries: Iterable[StateEntryT]): S
  def stateEntryFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[StateEntryT]
}
trait SingleEntryState extends MetadataProcess {
  override type StateEntryT = S

  def stateAsEntries(state: S): Iterator[S] = Iterator(state)
  def entriesAsState(entries: Iterable[S]): S = entries.head
  def stateEntryFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[S] = stateFormat
  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[S]
}

object MetadataProcess {
  sealed trait StreamEntry
  final case class Metadata(entry: MetadataEnvelope) extends StreamEntry
  case object AllObjectsReplayed extends StreamEntry
  case object MakeSnapshot extends StreamEntry
  case object ShuttingDown extends StreamEntry
  final case class Execute[S, T](f: S => (S, Vector[WorkEntry], T), promise: Promise[T]) extends StreamEntry

  def asSource(p: MetadataProcess, config: RepositoryConfig, journal: MetadataJournal, extractionEC: ExecutionContext)(implicit system: ActorSystem, ec: ExecutionContext): Source[WorkEntry, p.Api] = {
    val injectApi: Source[StreamEntry, p.Api] =
      Source.queue[StreamEntry](10000, OverflowStrategy.dropNew) // FIXME: will this be enough for mass injections?
        .mergeMat(MergeHub.source[StreamEntry])(Keep.both)
        .mapMaterializedValue {
          case (queue, sink) =>
            p.api(new HandleWithStateFunc[p.S] {
              def apply[T](f: p.S => (p.S, Vector[WorkEntry], T)): Future[T] = {
                val promise = Promise[T]
                queue.offer(Execute(f, promise))
                  .onComplete {
                    case Success(QueueOfferResult.Enqueued) =>
                    case x                                  => println(s"Injecting entries failed because of $x")
                  }
                promise.future
              }

              def handleStream: Sink[p.S => (p.S, Vector[WorkEntry]), Any] =
                Flow[p.S => (p.S, Vector[WorkEntry])]
                  .map { f =>
                    Execute[p.S, Unit]({ state =>
                      val (newState, ses) = f(state)
                      (newState, ses, ())
                    }, Promise.successful(()))
                  }
                  .to(sink)
            })
        }

    val extractionContext = new ExtractionContext {
      def executionContext: ExecutionContext = extractionEC
      def accessData[T](hash: Hash)(f: File => Future[T]): Future[T] =
        // FIXME: easy for now as we expect all hashes to be available as files
        f(config.fileInfoOf(hash).repoFile)
    }

    type Handler = ProcessState => StreamEntry => ProcessState
    case class ProcessState(seqNr: Long, processState: p.S, handler: Handler, workEntries: Vector[WorkEntry], lastSnapshotAt: Long, hasFinishedReplaying: Boolean, finished: Boolean) {
      def run(streamEntry: StreamEntry): ProcessState = handler(this)(streamEntry)

      def runEntry(envelope: MetadataEnvelope): ProcessState = copy(processState = p.processEvent(processState, envelope))

      def execute[T](e: Execute[p.S, T]): ProcessState =
        Try(e.f(processState)) match {
          case Success((newState, workEntries, res)) =>
            e.promise.trySuccess(res)
            this
              .withState(newState)
              .addWorkEntries(workEntries)
          case Failure(ex) =>
            e.promise.tryFailure(ex)
            this
        }

      def withSeqNr(newSeqNr: Long): ProcessState = copy(seqNr = newSeqNr)
      def withHandler(newHandler: Handler): ProcessState = copy(handler = newHandler)
      def withState(newState: p.S): ProcessState = copy(processState = newState)
      def withLastSnapshotAt(newValue: Long): ProcessState = copy(lastSnapshotAt = newValue)
      def withLastSnapshotNow: ProcessState = copy(lastSnapshotAt = seqNr)
      def addWorkEntries(newWorkEntries: Vector[WorkEntry]): ProcessState = copy(workEntries = workEntries ++ newWorkEntries)
      def clearWorkEntries: ProcessState = copy(workEntries = Vector.empty)

      def emit: (ProcessState, Vector[WorkEntry]) =
        if (hasFinishedReplaying)
          if (workEntries.nonEmpty) (clearWorkEntries, workEntries)
          else {
            val (newState, workEntries) = p.createWork(processState, extractionContext)
            (withState(newState), workEntries)
          }
        else
          (this, Vector.empty)

      def setFinishedReplaying: ProcessState = copy(hasFinishedReplaying = true)
      def setFinished: ProcessState = copy(finished = true)

      def saveSnapshot(force: Boolean = false): ProcessState =
        if ((lastSnapshotAt + config.snapshotOffset < seqNr) || (force && lastSnapshotAt < seqNr)) bench("Serializing state") {
          serializeState(p, config)(Snapshot(p.id, p.version, seqNr, processState))
          this.withLastSnapshotNow
        }
        else this
    }
    def bench[T](what: String)(f: => T): T = {
      val started = System.nanoTime()
      val res = f
      val lasted = System.nanoTime() - started
      println(s"[${p.id}] $what in ${lasted / 1000000} ms")
      res
    }
    def replaying(waitingExecutions: Vector[Execute[p.S, _]]): Handler = state => {
      case Metadata(e) =>
        if (e.seqNr == state.seqNr + 1) {
          state
            .runEntry(e)
            .withSeqNr(e.seqNr)
        } else if (e.seqNr < state.seqNr + 1)
          throw new IllegalStateException(s"Got unexpected duplicate seqNr ${e.seqNr} after last ${state.seqNr}")
        else
          throw new IllegalStateException(s"Got unexpected gap in seqNr ${e.seqNr} after last ${state.seqNr}")

      case AllObjectsReplayed =>
        println(s"[${p.id}] finished replaying after [${state.seqNr}]")

        // run all waiting executions
        waitingExecutions.foldLeft(state)(_.execute(_))
          .saveSnapshot()
          .setFinishedReplaying
          .withHandler(ignoreDuplicateSeqNrs)

      case e: Execute[p.S, t] @unchecked =>
        state.withHandler(replaying(waitingExecutions :+ e))

      case MakeSnapshot => state.saveSnapshot()
      case ShuttingDown => state.saveSnapshot(force = true).setFinished
    }
    def ignoreDuplicateSeqNrs: Handler = state => {
      case Metadata(e) if e.seqNr <= state.seqNr =>
        println(s"[${p.id}] at ${state.seqNr} got ${e.seqNr}. Ignoring")
        state
      case m: Metadata =>
        state
          .withHandler(liveEvents)
          .run(m)

      case e: Execute[p.S, t] @unchecked => state.execute(e)

      case MakeSnapshot                  => state.saveSnapshot()
      case ShuttingDown                  => state.saveSnapshot(force = true).setFinished
    }
    def liveEvents: Handler = state => {
      case Metadata(e) =>
        if (e.seqNr == state.seqNr + 1) {
          state
            .runEntry(e)
            .withSeqNr(e.seqNr)
        } else if (e.seqNr < state.seqNr + 1)
          throw new IllegalStateException(s"Got unexpected duplicate seqNr ${e.seqNr} after last ${state.seqNr}, ignoring...")
        else
          throw new IllegalStateException(s"Got unexpected gap in seqNr ${e.seqNr} after last ${state.seqNr}")

      case e: Execute[p.S, t] @unchecked => state.execute(e)

      case MakeSnapshot                  => state.saveSnapshot()
      case ShuttingDown                  => state.saveSnapshot(force = true).setFinished

      case AllObjectsReplayed            => throw new IllegalStateException("Unexpected AllObjectsReplayed in state liveEvents")
    }

    def statefulDetachedFlow[T, U, S](initialState: () => S, handle: (S, T) => S, emit: S => (S, Vector[U]), isFinished: S => Boolean): Flow[T, U, Any] =
      Flow.fromGraph(new StatefulDetachedFlow(initialState, handle, emit, isFinished))

    val snapshot = bench("Deserializing state")(deserializeState(p, config).getOrElse(Snapshot(p.id, p.version, -1L, p.initialState)))
    println(s"[${p.id}] initialized process at seqNr [${snapshot.currentSeqNr}]")

    val processFlow =
      Flow[StreamEntry]
        .merge(Source.tick(config.snapshotInterval, config.snapshotInterval, MakeSnapshot))
        .mergeMat(injectApi)(Keep.right)
        .via(statefulDetachedFlow[StreamEntry, WorkEntry, ProcessState](
          () => ProcessState(snapshot.currentSeqNr, snapshot.state, replaying(Vector.empty), Vector.empty, lastSnapshotAt = snapshot.currentSeqNr, hasFinishedReplaying = false, finished = false),
          _.run(_),
          _.emit,
          _.finished
        ))
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

  // FIXME: join with header
  case class Snapshot[S](processId: String, processVersion: Int, currentSeqNr: Long, state: S)
  import spray.json.DefaultJsonProtocol._
  import spray.json._
  private def processSnapshotFile(p: MetadataProcess, config: RepositoryConfig): File =
    new File(config.metadataDir, s"${p.id.replaceAll("""[\[\]]""", "_")}.snapshot.json.gz")

  private case class SnapshotHeader(processId: String, processVersion: Int, currentSeqNr: Long)
  private implicit val headerFormat = jsonFormat3(SnapshotHeader.apply)
  private def serializeState(p: MetadataProcess, config: RepositoryConfig)(snapshot: Snapshot[p.S]): Unit = {
    val targetFile = processSnapshotFile(p, config)
    val tmpFile = File.createTempFile(targetFile.getName, ".tmp", targetFile.getParentFile)
    val os = new GZIPOutputStream(new FileOutputStream(tmpFile))

    try {
      implicit val seFormat: JsonFormat[p.StateEntryT] = p.stateEntryFormat(config.entryFormat)
      os.write(SnapshotHeader(snapshot.processId, snapshot.processVersion, snapshot.currentSeqNr).toJson.compactPrint.getBytes("utf8"))
      os.write('\n')
      p.stateAsEntries(snapshot.state).foreach { entry =>
        os.write(entry.toJson.compactPrint.getBytes("utf8"))
        os.write('\n')
      }
    } finally os.close()
    Files.move(tmpFile.toPath, targetFile.toPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
  }
  def deserializeState(p: MetadataProcess, config: RepositoryConfig)(implicit system: ActorSystem): Option[Snapshot[p.S]] = {
    import system.dispatcher

    val file = processSnapshotFile(p, config)
    if (file.exists) {
      implicit val seFormat: JsonFormat[p.StateEntryT] = p.stateEntryFormat(config.entryFormat)

      val headerAndEntriesF =
        FileIO.fromPath(file.toPath)
          .via(Compression.gunzip())
          .via(Framing.delimiter(ByteString("\n"), 1000000000))
          .map(_.utf8String)
          .prefixAndTail(1)
          .runWith(Sink.head)
          .flatMap {
            case (Seq(header), entries) =>
              val h = header.parseJson.convertTo[SnapshotHeader]
              entries
                .map(_.parseJson.convertTo[p.StateEntryT])
                .runWith(Sink.seq)
                .map(es =>
                  Snapshot[p.S](h.processId, h.processVersion, h.currentSeqNr, p.entriesAsState(es))
                )
          }

      // FIXME
      val hs = Await.ready(headerAndEntriesF, 60.seconds)

      hs.value.get
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
    } else
      None
  }

}

object GetAllObjectsProcess extends MetadataProcess with SingleEntryState {
  case class State(knownHashes: Set[Hash])
  type S = State
  type Api = () => Future[Set[Hash]]

  def version: Int = 1

  def initialState: State = State(Set.empty)
  def processEvent(state: State, event: MetadataEnvelope): State =
    state.copy(knownHashes = state.knownHashes + event.entry.target.hash)
  def createWork(state: State, context: ExtractionContext): (State, Vector[WorkEntry]) = (state, Vector.empty)

  def api(handleWithState: HandleWithStateFunc[S])(implicit ec: ExecutionContext): Api =
    () => handleWithState { state =>
      (state, Vector.empty, state.knownHashes)
    }

  import spray.json.DefaultJsonProtocol._
  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[State] = jsonFormat1(State.apply)
}

trait Ingestion {
  def ingestionDataSink: Sink[(Hash, IngestionData), Any]
  def ingest(hash: Hash, data: IngestionData): Unit
}
object IngestionController extends MetadataProcess with SingleEntryState {
  case class State(datas: Map[Hash, Vector[IngestionData]]) {
    def add(hash: Hash, data: IngestionData): State =
      copy(datas = datas + (hash -> (datas.getOrElse(hash, Vector.empty) :+ data)))
  }
  type S = State
  type Api = Ingestion

  def version: Int = 1
  def initialState: State = State(Map.empty)

  def processEvent(state: State, event: MetadataEnvelope): State = event.entry match {
    case entry if entry.kind == IngestionData => state.add(entry.target.hash, entry.value.asInstanceOf[IngestionData])
    case _                                    => state
  }
  def createWork(state: State, context: ExtractionContext): (State, Vector[WorkEntry]) = (state, Vector.empty)

  def api(handleWithState: HandleWithStateFunc[S])(implicit ec: ExecutionContext): Ingestion = new Ingestion {
    def ingestionDataSink: Sink[(Hash, IngestionData), Any] =
      Flow[(Hash, IngestionData)]
        .map {
          case (hash, data) => handleNewEntry(hash, data)
        }
        .to(handleWithState.handleStream)

    def ingest(hash: Hash, newData: IngestionData): Unit =
      handleWithState { state =>
        val (newState, ses) = handleNewEntry(hash, newData)(state)
        (newState, ses, ())
      }

    private def handleNewEntry(hash: Hash, newData: IngestionData): S => (S, Vector[WorkEntry]) = state => {
      def matches(data: IngestionData): Boolean =
        newData.originalFullFilePath == data.originalFullFilePath

      val newEntries =
        if (!state.datas.get(hash).exists(_.exists(matches))) {
          println(s"Injecting [$newData]")
          //IngestionDataExtractor.extractMetadata(fi).toOption.toVector
          Vector(MetadataEntry(
            Id.Hashed(hash),
            Vector.empty,
            IngestionData,
            CreationInfo(DateTime.now, false, Ingestion),
            newData
          ))
        } else {
          //println(s"Did not ingest $fi because there already was an entry")
          Vector.empty
        }

      (state, Vector(() => Future.successful(newEntries)))
    }
  }
  import spray.json._
  import DefaultJsonProtocol._
  implicit val ingestionDataFormat = IngestionData.jsonFormat
  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[State] = jsonFormat1(State.apply)
}

trait MetadataExtractionScheduler {
  def workHistogram: Future[Map[String, Int]]
}

class MetadataIsCurrentProcess(extractor: MetadataExtractor) extends MetadataProcess with SingleEntryState {
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

        if (newL forall (_.exists)) Ready(newL.map(_.get))
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
  type Api = MetadataExtractionScheduler

  override val id: String = s"net.virtualvoid.fotofinish.metadata[${extractor.kind}]"
  def version: Int = 2

  def initialState: S = State(Map.empty)
  def processEvent(state: S, event: MetadataEnvelope): S = state.handle(event.entry)

  def createWork(state: S, context: ExtractionContext): (S, Vector[WorkEntry]) = {
    val (stateChanges: Vector[State => State], workEntries: Vector[WorkEntry] @unchecked) =
      state.objectStates
        .collect { case (hash, r: Ready) => (hash, r) }
        .take(10)
        .map {
          case (hash, r @ Ready(depValues)) =>
            extractor.precondition(hash, depValues, context) match {
              case None =>
                // precondition met
                (
                  (_: State).set(hash, Scheduled(depValues)),
                  (() => extractor.extract(hash, depValues, context).map(Vector(_))(context.executionContext)): WorkEntry
                )
              case Some(cause) =>
                (
                  (_: State).set(hash, PreConditionNotMet(cause)),
                  (() => Future.successful(Vector.empty)) // FIXME: is there a better way that doesn't involve scheduling a useless process?
                )
            }
        }.toVector.unzip
    if (stateChanges.isEmpty) (state, Vector.empty)
    else (stateChanges.foldLeft(state)((s, f) => f(s)), workEntries)
  }

  override def api(handleWithState: HandleWithStateFunc[State])(implicit ec: ExecutionContext): MetadataExtractionScheduler =
    new MetadataExtractionScheduler {
      def workHistogram: Future[Map[String, Int]] =
        handleWithState.access { state =>
          state.objectStates
            .groupBy(_._2.productPrefix)
            .mapValues(_.size)
        }
    }

  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[State] = {
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
    jsonFormat1(State.apply)
  }

  override def initializeStateSnapshot(state: State): State =
    state.mutateStates(_.mapValues {
      case Scheduled(depValues) => Ready(depValues) // throw away markers that a calculation is already in process
      case x                    => x
    })
}

trait MetadataApi {
  def metadataFor(id: Id): Future[Metadata]
  def knownObjects(): Future[TreeSet[Id]]
}

object PerObjectMetadataCollector extends MetadataProcess {
  sealed trait ParsedOrNot {
    def metadata: Metadata
  }
  case class Parsed(metadata: Metadata) extends ParsedOrNot
  case class NotParsed(data: String, f: () => Metadata) extends ParsedOrNot {
    override def metadata: Metadata = f()
  }
  case class State(
      metadata: TreeMap[Hash, ParsedOrNot]
  ) {
    def handle(entry: MetadataEntry): State = metadata.get(entry.target.hash) match {
      case Some(m) => State(metadata + (entry.target.hash -> Parsed(Metadata(m.metadata.entries :+ entry))))
      case None    => State(metadata + (entry.target.hash -> Parsed(Metadata(Vector(entry)))))
    }
    def metadataFor(hash: Hash): Metadata = metadata.get(hash).map(_.metadata).getOrElse(Metadata(Vector.empty))
    def knownHashes: Iterable[Hash] = metadata.keys
  }

  override type S = State
  override type Api = MetadataApi

  override def version: Int = 1

  override def initialState: State = State(TreeMap.empty)
  override def processEvent(state: State, event: MetadataEnvelope): State = state.handle(event.entry)
  override def createWork(state: State, context: ExtractionContext): (State, Vector[WorkEntry]) = (state, Vector.empty)
  override def api(handleWithState: HandleWithStateFunc[State])(implicit ec: ExecutionContext): MetadataApi =
    new MetadataApi {
      def metadataFor(id: Id): Future[Metadata] =
        handleWithState.access { state => state.metadataFor(id.hash) }
      override def knownObjects(): Future[TreeSet[Id]] =
        handleWithState.access { state => TreeSet(state.knownHashes.map(Hashed(_): Id).toVector: _*) }
    }

  override type StateEntryT = (Hash, ParsedOrNot)

  override def stateAsEntries(state: State): Iterator[(Hash, ParsedOrNot)] = state.metadata.iterator
  override def entriesAsState(entries: Iterable[(Hash, ParsedOrNot)]): State = State(TreeMap(entries.toSeq: _*))
  override def stateEntryFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[(Hash, ParsedOrNot)] = {
    import spray.json.DefaultJsonProtocol._
    import spray.json._
    /*implicit def treeMapFormat[K: Ordering: JsonFormat, V: JsonFormat]: JsonFormat[TreeMap[K, V]] =
      JsonExtra.deriveFormatFrom[Map[K, V]](_.toMap, m => TreeMap(m.toSeq: _*))*/

    implicit def metadataFormat: JsonFormat[Metadata] = jsonFormat1(Metadata.apply _)
    implicit def parsedOrNotFormat: JsonFormat[ParsedOrNot] = new JsonFormat[ParsedOrNot] {
      override def read(json: JsValue): ParsedOrNot = json match {
        case JsString(str) => NotParsed(str, () => str.parseJson.convertTo[Metadata])
        case _             => throw new IllegalStateException
      }
      override def write(obj: ParsedOrNot): JsValue = obj match {
        case NotParsed(str, f) => JsString(str)
        case Parsed(metadata)  => JsString(metadata.toJson.compactPrint)
      }
    }

    implicitly[JsonFormat[(Hash, ParsedOrNot)]]
  }
}
