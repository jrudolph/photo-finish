package net.virtualvoid.fotofinish.process

import net.virtualvoid.fotofinish.Hash
import net.virtualvoid.fotofinish.metadata.Id.Hashed
import net.virtualvoid.fotofinish.metadata.{ ExtractionContext, Id, Metadata, MetadataEntry, MetadataEnvelope }
import spray.json.JsonFormat

import scala.collection.immutable.TreeSet
import scala.concurrent.{ ExecutionContext, Future }

trait MetadataApi {
  def metadataFor(id: Id): Future[Metadata]
  def knownObjects(): Future[TreeSet[Id]]
}

object PerObjectMetadataCollector extends PerHashProcessWithNoGlobalState {
  type PerHashState = Metadata
  override type Api = MetadataApi

  def version: Int = 2

  def initialPerHashState(hash: Hash): Metadata = Metadata(Vector.empty)
  def processEvent(hash: Hash, state: Metadata, event: MetadataEnvelope): Effect =
    Effect.setHashState(hash, state.copy(entries = state.entries :+ event.entry))

  def hasWork(hash: Hash, state: Metadata): Boolean = false
  def createWork(hash: Hash, state: Metadata, context: ExtractionContext): (Metadata, Vector[WorkEntry]) = (state, Vector.empty)
  def api(handleWithState: PerHashHandleWithStateFunc[Metadata])(implicit ec: ExecutionContext): MetadataApi =
    new MetadataApi {
      def metadataFor(id: Id): Future[Metadata] =
        handleWithState.access(id.hash)(identity)
      override def knownObjects(): Future[TreeSet[Id]] =
        handleWithState.accessAllKeys { keys => TreeSet(keys.map(Hashed(_): Id).toVector: _*) }
    }
  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[Metadata] = {
    import spray.json.DefaultJsonProtocol._
    implicit def metadataFormat: JsonFormat[Metadata] = jsonFormat1(Metadata.apply _)
    implicitly[JsonFormat[Metadata]]
  }
}
