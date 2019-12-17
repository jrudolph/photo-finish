package net.virtualvoid.fotofinish

import java.io.{ File, FileOutputStream }

import akka.actor.ActorSystem
import akka.pattern.after
import akka.stream.scaladsl.{ BroadcastHub, Compression, FileIO, Flow, Framing, Keep, MergeHub, Source }
import akka.util.ByteString
import net.virtualvoid.fotofinish.MetadataProcess.SideEffect
import net.virtualvoid.fotofinish.metadata.{ MetadataEntry, MetadataExtractor, MetadataManager }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

trait MetadataProcess {
  type S
  type Api
  def initialState: S
  def processEvent(state: S, event: MetadataEntry): S
  def sideEffects(state: S, fileInfoFor: Hash => FileInfo)(implicit ec: ExecutionContext): (S, Vector[SideEffect])
  def api(stateAccess: () => Future[S])(implicit ec: ExecutionContext): Api

  // FIXME: add persistence
}

object MetadataProcess {
  // TODO: make those work items a proper trait that includes metadata about a work item
  // like expected workload in terms of CPU, IO, etc.
  type SideEffect = () => Future[Vector[MetadataEntry]]

  sealed trait StreamEntry
  final case class Metadata(entry: MetadataEntry) extends StreamEntry
  case object AllObjectsReplayed extends StreamEntry

  def asStream(p: MetadataProcess, manager: RepositoryManager)(implicit ec: ExecutionContext): Flow[StreamEntry, SideEffect, Any] =
    Flow[StreamEntry]
      .statefulMapConcat[SideEffect] { () =>
        var currentState = p.initialState
        var finishedReplaying: Boolean = false

        {
          case Metadata(e) =>
            println(s"Got metadata $e")
            // FIXME: check gaps and duplicate seqNrs
            currentState = p.processEvent(currentState, e)

            if (finishedReplaying) {
              val (newState, sideEffects) = p.sideEffects(currentState, manager.config.fileInfoOf)
              println(s"Got ${sideEffects.size} side effects for ${e.header.forData}")
              currentState = newState
              sideEffects
            } else Nil
          case AllObjectsReplayed => // FIXME: we might see some extra elements after we got this element so must sort them out
            println("Got AllObjectsReplayed")
            val (newState, sideEffects) = p.sideEffects(currentState, manager.config.fileInfoOf)
            currentState = newState
            finishedReplaying = true
            sideEffects
        }
      }

  /**
   * A flow that produces existing entries and consumes new events to be written to the journal.
   * The flow can be reused.
   */
  def journal(manager: RepositoryManager, meta: MetadataManager)(implicit system: ActorSystem): Flow[MetadataEntry, StreamEntry, Any] = {
    import system.dispatcher

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

    val (sink, source) =
      MergeHub.source[MetadataEntry]
        .via(liveJournalFlow)
        .toMat(BroadcastHub.sink[MetadataEntry](2048))(Keep.both)
        .run()

    Flow.fromSinkAndSource(
      sink,
      existingEntries
        .concat(source.map(Metadata))
    )
  }
}

object GetAllObjectsProcess extends MetadataProcess {
  case class State(knownHashes: Set[Hash])
  type S = State
  type Api = () => Future[Set[Hash]]

  def initialState: State = State(Set.empty)
  def processEvent(state: State, event: MetadataEntry): State =
    state.copy(knownHashes = state.knownHashes + event.header.forData)
  def sideEffects(state: State, fileInfoFor: Hash => FileInfo)(implicit ec: ExecutionContext): (State, Vector[SideEffect]) = (state, Vector.empty)
  def api(stateAccess: () => Future[State])(implicit ec: ExecutionContext): () => Future[Set[Hash]] =
    () => stateAccess().map(_.knownHashes)
}

class MetadataIsCurrentProcess(extractor: MetadataExtractor) extends MetadataProcess {
  sealed trait HashState
  case object Known extends HashState
  case object Scheduled extends HashState
  case class Calculated(version: Int) extends HashState

  override type S = Map[Hash, HashState]
  override type Api = Unit

  override def initialState: S = Map.empty

  override def processEvent(state: S, event: MetadataEntry): S = {
    val hash = event.header.forData
    if (event.extractor == extractor) {
      val newState =
        state.get(hash) match {
          case Some(Calculated(otherVersion)) => Calculated(otherVersion max event.header.version)
          case _                              => Calculated(event.header.version)
        }

      state.updated(hash, newState)
    } else if (state.contains(hash)) state
    else state.updated(hash, Known)
  }

  override def sideEffects(state: S, fileInfoFor: Hash => FileInfo)(implicit ec: ExecutionContext): (S, Vector[SideEffect]) = {
    val toSchedule = state.filter(_._2 == Known)
    if (toSchedule.isEmpty) (state, Vector.empty)
    else {
      (
        state ++ toSchedule.mapValues(_ => Scheduled),
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

  override def api(stateAccess: () => Future[S])(implicit ec: ExecutionContext): Unit = ()
}
