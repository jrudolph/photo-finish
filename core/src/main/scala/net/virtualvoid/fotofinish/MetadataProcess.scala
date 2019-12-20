package net.virtualvoid.fotofinish

import java.io.{ File, FileInputStream, FileOutputStream }
import java.util.zip.{ GZIPInputStream, GZIPOutputStream }

import akka.actor.ActorSystem
import akka.pattern.after
import akka.stream.scaladsl.{ BroadcastHub, Compression, FileIO, Flow, Framing, Keep, MergeHub, Sink, Source }
import akka.stream.{ KillSwitches, OverflowStrategy, QueueOfferResult }
import akka.util.ByteString
import net.virtualvoid.fotofinish.MetadataProcess.SideEffect
import net.virtualvoid.fotofinish.metadata._
import spray.json.JsonFormat

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.language.implicitConversions
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

trait HandleWithStateFunc[S] {
  def apply[T](f: S => (S, Vector[SideEffect], T)): Future[T]
}

trait MetadataProcess {
  type S
  type Api

  def id: String = getClass.getName
  def version: Int

  def initialState: S
  def processEvent(state: S, event: MetadataEntry): S
  def sideEffects(state: S, fileInfoFor: Hash => FileInfo)(implicit ec: ExecutionContext): (S, Vector[SideEffect])
  def api(handleWithState: HandleWithStateFunc[S])(implicit ec: ExecutionContext): Api

  def stateFormat: JsonFormat[S]
  /** Allows to prepare state loaded from snapshot */
  def initializeStateSnapshot(state: S): S = state
}

object MetadataProcess {
  // TODO: make those work items a proper trait that includes metadata about a work item
  // like expected workload in terms of CPU, IO, etc.
  type SideEffect = () => Future[Vector[MetadataEntry]]

  sealed trait StreamEntry
  final case class Metadata(entry: MetadataEntry) extends StreamEntry
  case object AllObjectsReplayed extends StreamEntry
  case object ShuttingDown extends StreamEntry
  final case class Execute[S, T](f: S => (S, Vector[SideEffect], T), promise: Promise[T]) extends StreamEntry

  def asSource(p: MetadataProcess, manager: RepositoryManager, journal: Journal)(implicit ec: ExecutionContext): Source[SideEffect, p.Api] =
    journal.source(-1)
      .viaMat(MetadataProcess.asStream(p, Settings.manager))(Keep.right)

  def asStream(p: MetadataProcess, manager: RepositoryManager)(implicit ec: ExecutionContext): Flow[StreamEntry, SideEffect, p.Api] = {
    val injectApi: Source[StreamEntry, p.Api] =
      Source.queue[StreamEntry](1000, OverflowStrategy.dropNew) // FIXME: will this be enough for mass injections?
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

    Flow[StreamEntry]
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

        def skipOverSnapshot(waitingExecutions: Vector[Execute[p.S, _]]): Handler =
          if (currentSeqNr == -1) replaying(waitingExecutions)
          else {
            case Metadata(entry) if entry.seqNr == currentSeqNr => replaying(waitingExecutions)
            case _: Metadata                                    => Effect.empty
            case e: Execute[p.S, t] @unchecked                  => Effect.changeHandler(skipOverSnapshot(waitingExecutions :+ e))
            case ShuttingDown                                   => closing
          }

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

            val (newState0, sideEffects0) = p.sideEffects(currentState, manager.config.fileInfoOf)

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

          case ShuttingDown => shutdown()
        }
        def liveEvents: Handler = {
          case Metadata(e) =>
            if (e.seqNr == currentSeqNr + 1) {
              val newState0 = p.processEvent(currentState, e)
              val (newState1, sideEffects) = p.sideEffects(newState0, manager.config.fileInfoOf)
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

          case ShuttingDown       => shutdown()

          case AllObjectsReplayed => throw new IllegalStateException("Unexpected AllObjectsReplayed in state liveEvents")
        }
        def shutdown(): Effect = {
          serializeState(p, manager)(Snapshot(p.id, p.version, currentSeqNr, currentState))
          closing
        }
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

        val snapshot =
          deserializeState(p, manager).getOrElse(Snapshot(p.id, p.version, -1L, p.initialState))
        println(s"[${p.id}] initialized process at seqNr [${snapshot.currentSeqNr}]")
        _curSeqNr = snapshot.currentSeqNr
        _curState = snapshot.state
        var curHandler: Handler = skipOverSnapshot(Vector.empty)

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
      implicit val sFormat: JsonFormat[p.S] = p.stateFormat
      os.write(snapshot.toJson.compactPrint.getBytes("utf8"))
    } finally os.close()
  }
  private def deserializeState(p: MetadataProcess, manager: RepositoryManager): Option[Snapshot[p.S]] = {
    val file = processSnapshotFile(p, manager)
    if (file.exists) {
      val fis = new FileInputStream(file)
      try {
        val is = new GZIPInputStream(fis)
        implicit val sFormat: JsonFormat[p.S] = p.stateFormat
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
  def journal(manager: RepositoryManager, meta: MetadataManager)(implicit system: ActorSystem): Journal = {
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

    val liveJournalFlow: Flow[MetadataEntry, MetadataEntry, Any] =
      Flow[MetadataEntry]
        .statefulMapConcat[MetadataEntry] { () =>
          var lastSeqNo: Long = readSeqNr()

          entry => {
            lastSeqNo += 1

            val entryWithSeqNr = entry.withSeqNr(lastSeqNo)
            writeSeqNr(lastSeqNo)
            meta.storeToDefaultDestinations(entryWithSeqNr)

            entryWithSeqNr :: Nil
          }
        }

    def readAllEntries(): Future[Source[MetadataEntry, Any]] =
      // add some delay which should help to make sure that live stream is running and connected when we start reading the
      // file
      after(100.millis, system.scheduler) {
        Future.successful {
          if (manager.config.allMetadataFile.exists())
            FileIO.fromPath(manager.config.allMetadataFile.toPath)
              .via(Compression.gunzip())
              .via(Framing.delimiter(ByteString("\n"), 1000000))
              .map(_.utf8String)
              .mapConcat(MetadataManager.readMetadataEntry(_).toVector)
          else
            Source.empty
        }
      }

    val existingEntries: Source[StreamEntry, Any] =
      Source.lazyFutureSource(readAllEntries)
        .map[StreamEntry](Metadata)
        .concat(Source.single(AllObjectsReplayed))

    val (liveSink, liveSource) =
      MergeHub.source[MetadataEntry]
        .via(liveJournalFlow)
        .via(killSwitch.flow)
        .toMat(BroadcastHub.sink[MetadataEntry](2048))(Keep.both)
        .run()

    new Journal {
      override def newEntrySink: Sink[MetadataEntry, Any] = liveSink
      override def source(fromSeqNr: Long): Source[StreamEntry, Any] =
        existingEntries
          .concat(liveSource.map(Metadata))
          .concat(Source.single(ShuttingDown))
      override def shutdown(): Unit = killSwitch.shutdown()
    }
  }
}

object GetAllObjectsProcess extends MetadataProcess {
  case class State(knownHashes: Set[Hash])
  type S = State
  type Api = () => Future[Set[Hash]]

  def version: Int = 1

  def initialState: State = State(Set.empty)
  def processEvent(state: State, event: MetadataEntry): State =
    state.copy(knownHashes = state.knownHashes + event.header.forData)
  def sideEffects(state: State, fileInfoFor: Hash => FileInfo)(implicit ec: ExecutionContext): (State, Vector[SideEffect]) = (state, Vector.empty)

  def api(handleWithState: HandleWithStateFunc[S])(implicit ec: ExecutionContext): Api =
    () => handleWithState { state =>
      (state, Vector.empty, state.knownHashes)
    }

  import spray.json.DefaultJsonProtocol._
  lazy val stateFormat: JsonFormat[State] = jsonFormat1(State.apply)
}

class IngestionController extends MetadataProcess {
  case class State(datas: Map[Hash, Vector[IngestionData]]) {
    def add(hash: Hash, data: IngestionData): State =
      copy(datas = datas + (hash -> (datas.getOrElse(hash, Vector.empty) :+ data)))
  }
  type S = State
  type Api = FileInfo => Unit

  override def version: Int = 1
  override def initialState: State = State(Map.empty)

  override def processEvent(state: State, event: MetadataEntry): State = event match {
    case entry if entry.extractor == IngestionDataExtractor => state.add(entry.header.forData, entry.data.asInstanceOf[IngestionData])
    case _ => state
  }
  override def sideEffects(state: State, fileInfoFor: Hash => FileInfo)(implicit ec: ExecutionContext): (State, Vector[SideEffect]) = (state, Vector.empty)

  def api(handleWithState: HandleWithStateFunc[S])(implicit ec: ExecutionContext): FileInfo => Unit = { fi =>
    def matches(data: IngestionData): Boolean =
      fi.originalFile.exists(f => f.getName == data.originalFileName && f.getParent == data.originalFilePath)

    handleWithState { state =>
      println(s"Checking if $fi needs ingesting...")

      val newEntries =
        if (!state.datas.get(fi.hash).exists(_.exists(matches))) {
          println(s"Injecting [${state.datas.get(fi.hash)}]")
          IngestionDataExtractor.extractMetadata(fi).toOption.toVector
        } else {
          println(s"Did not ingest $fi because there already was an entry")
          Vector.empty
        }

      (state, Vector(() => Future.successful(newEntries)), ())
    }
  }
  import spray.json._
  import DefaultJsonProtocol._
  implicit val idFormat = IngestionDataExtractor.metadataFormat
  val stateFormat: JsonFormat[State] = jsonFormat1(State.apply)
}

class MetadataIsCurrentProcess(extractor: MetadataExtractor) extends MetadataProcess {
  sealed trait HashState
  case object Known extends HashState
  case object Scheduled extends HashState
  case class Calculated(version: Int) extends HashState

  case class State(objectStates: Map[Hash, HashState]) {
    def get(hash: Hash): Option[HashState] = objectStates.get(hash)
    def set(hash: Hash, newState: HashState): State = mutateStates(_.updated(hash, newState))

    def mutateStates(f: Map[Hash, HashState] => Map[Hash, HashState]): State =
      copy(objectStates = f(objectStates))
  }

  type S = State
  type Api = Unit

  override val id: String = s"net.virtualvoid.fotofinish.metadata[${extractor.kind}]"
  def version: Int = 1

  def initialState: S = State(Map.empty)

  def processEvent(state: S, event: MetadataEntry): S = {
    val hash = event.header.forData
    if (event.extractor == extractor) {
      val newState =
        state.get(hash) match {
          case Some(Calculated(otherVersion)) => Calculated(otherVersion max event.header.version)
          case _                              => Calculated(event.header.version)
        }

      state.set(hash, newState)
    } else if (state.objectStates.contains(hash)) state
    else state.set(hash, Known)
  }

  def sideEffects(state: S, fileInfoFor: Hash => FileInfo)(implicit ec: ExecutionContext): (S, Vector[SideEffect]) = {
    val toSchedule = state.objectStates.filter(_._2 == Known)
    if (toSchedule.isEmpty) (state, Vector.empty)
    else {
      (
        state.mutateStates(_ ++ toSchedule.mapValues(_ => Scheduled)),
        toSchedule.keys.toVector.map { hash => () =>
          Future {
            val res = extractor.extractMetadata(fileInfoFor(hash))
            println(s"Executing extractor for $hash produced $res")
            Vector(res.get)
          }
        }
      )
    }
  }

  def api(handleWithState: HandleWithStateFunc[State])(implicit ec: ExecutionContext): Unit = ()

  import spray.json.DefaultJsonProtocol._
  import spray.json._

  private implicit def calculatedFormat: JsonFormat[Calculated] = jsonFormat1(Calculated.apply)
  private implicit def hashStateFormat: JsonFormat[HashState] = new JsonFormat[HashState] {
    import net.virtualvoid.fotofinish.util.JsonExtra._
    override def read(json: JsValue): HashState =
      json.field("type") match {
        case JsString("Known")      => Known
        case JsString("Scheduled")  => Scheduled
        case JsString("Calculated") => json.convertTo[Calculated]
        case x                      => throw DeserializationException(s"Unexpected type '$x' for HashState")
      }

    override def write(obj: HashState): JsValue = obj match {
      case Known         => JsObject("type" -> JsString("Known"))
      case Scheduled     => JsObject("type" -> JsString("Scheduled"))
      case c: Calculated => c.toJson + ("type" -> JsString("Calculated"))
    }
  }
  def stateFormat: JsonFormat[State] = jsonFormat1(State.apply)

  override def initializeStateSnapshot(state: State): State =
    state.mutateStates(_.mapValues {
      case Scheduled => Known // throw away markers that a calculation is already in process
      case x         => x
    })
}
