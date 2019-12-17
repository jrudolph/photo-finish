package net.virtualvoid.fotofinish

import akka.stream.scaladsl.Flow
import net.virtualvoid.fotofinish.MetadataProcess.SideEffect
import net.virtualvoid.fotofinish.metadata.{ MetadataEntry, MetadataExtractor }

import scala.concurrent.{ ExecutionContext, Future }

trait MetadataProcess {
  type S
  type Api
  def initialState: S
  def processEvent(state: S, event: MetadataEntry): S
  def sideEffects(state: S)(implicit ec: ExecutionContext): (S, Vector[SideEffect])
  def api(stateAccess: () => Future[S])(implicit ec: ExecutionContext): Api
}

object MetadataProcess {
  // TODO: make those work items a proper trait that includes metadata about a work item
  // like expected workload in terms of CPU, IO, etc.
  type SideEffect = () => Future[Vector[MetadataEntry]]

  sealed trait StreamEntry
  final case class Metadata(entry: MetadataEntry) extends StreamEntry
  case object AllObjectsReplayed extends StreamEntry

  def asStream(p: MetadataProcess)(implicit ec: ExecutionContext): Flow[StreamEntry, SideEffect, Any] =
    Flow[StreamEntry]
      .statefulMapConcat[SideEffect] { () =>
        var currentState = p.initialState
        var finishedReplaying: Boolean = false

        {
          case Metadata(e) =>
            currentState = p.processEvent(currentState, e)

            if (finishedReplaying) {
              val (newState, sideEffects) = p.sideEffects(currentState)
              currentState = newState
              sideEffects
            } else Nil
          case AllObjectsReplayed =>
            val (newState, sideEffects) = p.sideEffects(currentState)
            currentState = newState
            finishedReplaying = true
            sideEffects
        }
      }
}

object GetAllObjectsProcess extends MetadataProcess {
  case class State(knownHashes: Set[Hash])
  type S = State
  type Api = () => Future[Set[Hash]]

  def initialState: State = State(Set.empty)
  def processEvent(state: State, event: MetadataEntry): State =
    state.copy(knownHashes = state.knownHashes + event.header.forData)
  def sideEffects(state: State)(implicit ec: ExecutionContext): (State, Vector[SideEffect]) = (state, Vector.empty)
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

  override def sideEffects(state: S)(implicit ec: ExecutionContext): (S, Vector[SideEffect]) = {
    val toSchedule = state.filter(_._2 == Known)
    if (toSchedule.isEmpty) (state, Vector.empty)
    else {
      (
        state ++ toSchedule.mapValues(_ => Scheduled),
        toSchedule.keys.toVector.map { hash => () =>
          Future {
            Vector(extractor.extractMetadata(FileInfo(hash, null, null)).get)
          }
        }
      )
    }
  }

  override def api(stateAccess: () => Future[S])(implicit ec: ExecutionContext): Unit = ()
}