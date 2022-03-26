package net.virtualvoid.fotofinish.process

import net.virtualvoid.fotofinish.metadata.{ ExtractionContext, MetadataEntry, MetadataEnvelope, MetadataKind }
import spray.json.JsonFormat

import scala.concurrent.{ ExecutionContext, Future }

class SimpleAggregationProcess[_S, T](override val id: String, val version: Int, kind: MetadataKind.Aux[T], val initialState: _S, f: (_S, MetadataEntry.Aux[T]) => _S)(implicit _stateFormat: JsonFormat[_S]) extends SingleEntryState {
  override type S = _S
  override type Api = () => Future[S]

  def processEvent(state: _S, event: MetadataEnvelope): _S =
    if (event.entry.kind == kind) f(state, event.entry.cast(kind))
    else state

  def createWork(state: _S): (_S, Vector[WorkEntry]) = (state, Vector.empty)
  def api(handleWithState: HandleWithStateFunc[_S])(implicit ec: ExecutionContext): () => Future[_S] =
    () => handleWithState.access(identity)
  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[_S] = _stateFormat
}
