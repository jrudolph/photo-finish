package net.virtualvoid.fotofinish

import java.io.{ File, FileOutputStream }
import java.nio.file.{ Files, StandardCopyOption }
import java.util.zip.{ GZIPOutputStream, ZipException }

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

import scala.collection.immutable.{ TreeMap, TreeSet }
import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

trait HandleWithStateFunc[S] {
  def apply[T](f: S => (S, Vector[SideEffect], T)): Future[T]
  def access[T](f: S => T): Future[T] =
    apply { state =>
      (state, Vector.empty, f(state))
    }

  def handleStream: Sink[S => (S, Vector[SideEffect]), Any]
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
  // TODO: make those work items a proper trait that includes metadata about a work item
  // like expected workload in terms of CPU, IO, etc.
  type SideEffect = () => Future[Vector[MetadataEntry]]

  sealed trait StreamEntry
  final case class Metadata(entry: MetadataEnvelope) extends StreamEntry
  case object AllObjectsReplayed extends StreamEntry
  case object MakeSnapshot extends StreamEntry
  case object ShuttingDown extends StreamEntry
  final case class Execute[S, T](f: S => (S, Vector[SideEffect], T), promise: Promise[T]) extends StreamEntry

  def asSource(p: MetadataProcess, config: RepositoryConfig, journal: Journal, extractionEC: ExecutionContext)(implicit system: ActorSystem, ec: ExecutionContext): Source[SideEffect, p.Api] = {
    val injectApi: Source[StreamEntry, p.Api] =
      Source.queue[StreamEntry](10000, OverflowStrategy.dropNew) // FIXME: will this be enough for mass injections?
        .mergeMat(MergeHub.source[StreamEntry])(Keep.both)
        .mapMaterializedValue {
          case (queue, sink) =>
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

              def handleStream: Sink[p.S => (p.S, Vector[SideEffect]), Any] =
                Flow[p.S => (p.S, Vector[SideEffect])]
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
    case class ProcessState(seqNr: Long, processState: p.S, handler: Handler, sideEffects: Vector[SideEffect], lastSnapshotAt: Long, hasFinishedReplaying: Boolean, finished: Boolean) {
      def run(streamEntry: StreamEntry): ProcessState = handler(this)(streamEntry)

      def runEntry(envelope: MetadataEnvelope): ProcessState = copy(processState = p.processEvent(processState, envelope))

      def execute[T](e: Execute[p.S, T]): ProcessState =
        Try(e.f(processState)) match {
          case Success((newState, sideEffects, res)) =>
            e.promise.trySuccess(res)
            this
              .withState(newState)
              .addSideEffects(sideEffects)
          case Failure(ex) =>
            e.promise.tryFailure(ex)
            this
        }

      def withSeqNr(newSeqNr: Long): ProcessState = copy(seqNr = newSeqNr)
      def withHandler(newHandler: Handler): ProcessState = copy(handler = newHandler)
      def withState(newState: p.S): ProcessState = copy(processState = newState)
      def withLastSnapshotAt(newValue: Long): ProcessState = copy(lastSnapshotAt = newValue)
      def withLastSnapshotNow: ProcessState = copy(lastSnapshotAt = seqNr)
      def addSideEffects(newSideEffects: Vector[SideEffect]): ProcessState = copy(sideEffects = sideEffects ++ newSideEffects)
      def clearSideEffects: ProcessState = copy(sideEffects = Vector.empty)

      def emit: (ProcessState, Vector[SideEffect]) =
        if (hasFinishedReplaying)
          if (sideEffects.nonEmpty) (clearSideEffects, sideEffects)
          else {
            val (newState, sideEffects) = p.sideEffects(processState, extractionContext)
            (withState(newState), sideEffects)
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
        .via(statefulDetachedFlow[StreamEntry, SideEffect, ProcessState](
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

  trait Journal {
    def newEntrySink: Sink[MetadataEntry, Any]
    def source(fromSeqNr: Long): Source[StreamEntry, Any]
    def shutdown(): Unit
  }

  /**
   * A flow that produces existing entries and consumes new events to be written to the journal.
   * The flow can be reused.
   */
  def journal(config: RepositoryConfig)(implicit system: ActorSystem): Journal = {
    import system.dispatcher

    val killSwitch = KillSwitches.shared("kill-journal")
    val seqNrFile = new File(config.metadataDir, "metadata.seqnr.txt")
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
              writeJournalEntry(config, entryWithSeqNr)
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
          if (config.allMetadataFile.exists())
            FileIO.fromPath(config.allMetadataFile.toPath)
              .via(Compression.gunzip())
              .via(Framing.delimiter(ByteString("\n"), 1000000))
              .map(_.utf8String)
              .mapConcat(readJournalEntry(config, _).toVector)
              .recoverWithRetries(1, {
                case z: ZipException => Source.empty
              })
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
          Source.lazyFutureSource(readAllEntries)
            .map[StreamEntry](Metadata)
            .concat(Source.single(AllObjectsReplayed))
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

  private def readJournalEntry(config: RepositoryConfig, entry: String): Option[MetadataEnvelope] = {
    import config.entryFormat
    try Some(entry.parseJson.convertTo[MetadataEnvelope])
    catch {
      case NonFatal(ex) =>
        println(s"Couldn't read [$entry] because of ${ex.getMessage}")
        ex.printStackTrace()
        None
    }
  }

  private def writeJournalEntry(config: RepositoryConfig, envelope: MetadataEnvelope): Unit = {
    val fos = new FileOutputStream(config.allMetadataFile, true)
    val out = new GZIPOutputStream(fos)
    import config.entryFormat
    out.write(envelope.toJson.compactPrint.getBytes("utf8"))
    out.write('\n')
    out.close()
    fos.close()
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
  def sideEffects(state: State, context: ExtractionContext): (State, Vector[SideEffect]) = (state, Vector.empty)

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
  def sideEffects(state: State, context: ExtractionContext): (State, Vector[SideEffect]) = (state, Vector.empty)

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

    private def handleNewEntry(hash: Hash, newData: IngestionData): S => (S, Vector[SideEffect]) = state => {
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

  def sideEffects(state: S, context: ExtractionContext): (S, Vector[SideEffect]) = {
    val (stateChanges: Vector[State => State], sideeffects: Vector[SideEffect]) =
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
  override def sideEffects(state: State, context: ExtractionContext): (State, Vector[SideEffect]) = (state, Vector.empty)
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
    import spray.json._
    import spray.json.DefaultJsonProtocol._
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

class StatefulDetachedFlow[T, U, S](initialState: () => S, handle: (S, T) => S, emitF: S => (S, Vector[U]), isFinished: S => Boolean) extends GraphStage[FlowShape[T, U]] {
  val in = Inlet[T]("StateFullDetachedFlow.in")
  val out = Outlet[U]("StateFullDetachedFlow.out")
  val shape: FlowShape[T, U] = FlowShape(in, out)
  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
    setHandlers(in, out, this)

    override def preStart(): Unit = pull(in)

    private[this] var state = initialState()

    override def onPush(): Unit = {
      state = handle(state, grab(in))
      if (isFinished(state)) completeStage()
      else {
        pull(in)
        if (isAvailable(out)) onPull()
      }
    }
    override def onPull(): Unit = {
      val (newState, toEmit) = emitF(state)
      state = newState
      if (toEmit.nonEmpty) emitMultiple(out, toEmit)
      if (isFinished(state)) completeStage()
    }
  }
}