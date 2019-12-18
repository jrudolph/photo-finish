package net.virtualvoid.fotofinish

import java.io.{ File, FileInputStream, FileOutputStream }
import java.util.zip.{ GZIPInputStream, GZIPOutputStream }

import akka.actor.ActorSystem
import akka.pattern.after
import akka.stream.scaladsl.{ BroadcastHub, Compression, FileIO, Flow, Framing, Keep, MergeHub, Source }
import akka.stream.{ KillSwitch, KillSwitches }
import akka.util.ByteString
import net.virtualvoid.fotofinish.MetadataProcess.SideEffect
import net.virtualvoid.fotofinish.metadata.{ MetadataEntry, MetadataExtractor, MetadataManager }
import spray.json.JsonFormat

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try
import scala.util.control.NonFatal

trait MetadataProcess {
  type S
  type Api

  def id: String = getClass.getName
  def version: Int

  def initialState: S
  def processEvent(state: S, event: MetadataEntry): S
  def sideEffects(state: S, fileInfoFor: Hash => FileInfo)(implicit ec: ExecutionContext): (S, Vector[SideEffect])
  def api(stateAccess: () => Future[S])(implicit ec: ExecutionContext): Api

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

  def asStream(p: MetadataProcess, manager: RepositoryManager)(implicit ec: ExecutionContext): Flow[StreamEntry, SideEffect, Any] =
    Flow[StreamEntry]
      .statefulMapConcat[SideEffect] { () =>
        abstract class Handler {
          def apply(e: StreamEntry): (immutable.Iterable[SideEffect], Handler)
        }
        def skipOverSnapshot(currentSeqNr: Long, currentState: p.S): Handler =
          if (currentSeqNr == -1) replaying(currentSeqNr, currentState)
          else {
            case Metadata(entry) if entry.seqNr == currentSeqNr => (Nil, replaying(currentSeqNr, currentState))
            case m: Metadata                                    => (Nil, skipOverSnapshot(currentSeqNr, currentState))
            case ShuttingDown                                   => (Nil, closing)
          }

        def replaying(currentSeqNr: Long, currentState: p.S): Handler = {
          case Metadata(e) =>
            if (e.seqNr == currentSeqNr + 1) {
              val newState = p.processEvent(currentState, e)
              (Nil, replaying(e.seqNr, newState))
            } else if (e.seqNr < currentSeqNr + 1)
              throw new IllegalStateException(s"Got unexpected duplicate seqNr ${e.seqNr} after last $currentSeqNr, ignoring...")
            else {
              println(s"Got unexpected gap in seqNr ${e.seqNr} after last $currentSeqNr, ignoring...")
              (Nil, replaying(e.seqNr, currentState))
            }

          case AllObjectsReplayed =>
            val (newState, sideEffects) = p.sideEffects(currentState, manager.config.fileInfoOf)
            (sideEffects, ignoreDuplicateSeqNrs(currentSeqNr, newState))

          case ShuttingDown => shutdown(currentSeqNr, currentState)
        }
        def ignoreDuplicateSeqNrs(currentSeqNr: Long, currentState: p.S): Handler = {
          case Metadata(e) if e.seqNr <= currentSeqNr => (Nil, ignoreDuplicateSeqNrs(currentSeqNr, currentState))
          case m: Metadata                            => liveEvents(currentSeqNr, currentState)(m)
          case ShuttingDown                           => shutdown(currentSeqNr, currentState)
        }
        def liveEvents(currentSeqNr: Long, currentState: p.S): Handler = {
          case Metadata(e) =>
            if (e.seqNr == currentSeqNr + 1) {
              val newState0 = p.processEvent(currentState, e)
              val (newState1, sideEffects) = p.sideEffects(newState0, manager.config.fileInfoOf)
              (sideEffects, liveEvents(e.seqNr, newState1))
            } else if (e.seqNr < currentSeqNr + 1)
              throw new IllegalStateException(s"Got unexpected duplicate seqNr ${e.seqNr} after last $currentSeqNr, ignoring...")
            else {
              println(s"Got unexpected gap in seqNr ${e.seqNr} after last $currentSeqNr, ignoring...")
              (Nil, liveEvents(e.seqNr, currentState))
            }
          case ShuttingDown => shutdown(currentSeqNr, currentState)
        }
        def shutdown(currentSeqNr: Long, currentState: p.S): (immutable.Iterable[SideEffect], Handler) = {
          serializeState(p, manager)(Snapshot(p.id, p.version, currentSeqNr, currentState))
          (Nil, closing)
        }
        lazy val closing: Handler = {
          case x => (Nil, closing)
        }

        val snapshot =
          deserializeState(p, manager).getOrElse(Snapshot(p.id, p.version, -1L, p.initialState))
        println("Initialized process")
        var curHandler: Handler = skipOverSnapshot(snapshot.currentSeqNr, snapshot.state)

        e => {
          val (els, newHandler) = curHandler(e)
          curHandler = newHandler
          els
        }
      }
      .recoverWithRetries(1, {
        case ex =>
          println(s"Process for [${p.id}] failed with [${ex.getMessage}]. Aborting the process.")
          ex.printStackTrace()
          Source.empty
      })

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

  /**
   * A flow that produces existing entries and consumes new events to be written to the journal.
   * The flow can be reused.
   */
  def journal(manager: RepositoryManager, meta: MetadataManager)(implicit system: ActorSystem): (KillSwitch, Flow[MetadataEntry, StreamEntry, Any]) = {
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

    (
      killSwitch,
      Flow.fromSinkAndSource(
        liveSink,
        existingEntries
          .concat(liveSource.map(Metadata))
          .concat(Source.single(ShuttingDown))
      )
    )
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
  def api(stateAccess: () => Future[State])(implicit ec: ExecutionContext): () => Future[Set[Hash]] =
    () => stateAccess().map(_.knownHashes)

  import spray.json.DefaultJsonProtocol._
  lazy val stateFormat: JsonFormat[State] = jsonFormat1(State.apply)
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

  def api(stateAccess: () => Future[S])(implicit ec: ExecutionContext): Unit = ()

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
