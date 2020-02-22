package net.virtualvoid.fotofinish.process

import net.virtualvoid.fotofinish.metadata.{ ExtractionContext, Id, Metadata, MetadataEntry, MetadataEnvelope }
import spray.json.JsonFormat

import scala.collection.immutable.TreeSet
import scala.concurrent.{ ExecutionContext, Future }

trait MetadataApi {
  def metadataFor(id: Id): Future[Metadata]
  def knownObjects(): Future[TreeSet[Id]]
}

object PerObjectMetadataCollector extends PerIdProcessWithNoGlobalState {
  type PerKeyState = Metadata
  override type Api = MetadataApi

  def version: Int = 2

  def initialPerKeyState(id: Id): Metadata = Metadata(Vector.empty)
  def processIdEvent(id: Id, state: Metadata, event: MetadataEnvelope): Effect =
    Effect.setKeyState(id, state.copy(entries = state.entries :+ event.entry))

  def hasWork(id: Id, state: Metadata): Boolean = false
  def createWork(id: Id, state: Metadata, context: ExtractionContext): (Metadata, Vector[WorkEntry]) = (state, Vector.empty)
  def api(handleWithState: PerIdHandleWithStateFunc[Metadata])(implicit ec: ExecutionContext): MetadataApi =
    new MetadataApi {
      def metadataFor(id: Id): Future[Metadata] =
        handleWithState.access(id)(identity)
      override def knownObjects(): Future[TreeSet[Id]] =
        handleWithState.accessAllKeys { keys => TreeSet(keys.toVector: _*) }
    }
  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[Metadata] = {
    import spray.json.DefaultJsonProtocol._
    implicit def metadataFormat: JsonFormat[Metadata] = jsonFormat1(Metadata.apply _)
    implicitly[JsonFormat[Metadata]]
  }
}
