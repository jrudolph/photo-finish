package net.virtualvoid.fotofinish.process

import akka.http.scaladsl.model.DateTime
import akka.stream.scaladsl.{ Flow, Sink }
import net.virtualvoid.fotofinish.Hash
import net.virtualvoid.fotofinish.metadata.{ CreationInfo, ExtractionContext, Id, Ingestion, IngestionData, MetadataEntry, MetadataEnvelope }
import spray.json.JsonFormat

import scala.concurrent.{ ExecutionContext, Future }

trait Ingestion {
  def ingestionDataSink: Sink[(Hash, IngestionData), Any]
  def ingest(hash: Hash, data: IngestionData): Unit
}

object PerHashIngestionController extends SimplePerHashProcess {
  type PerHashState = Vector[IngestionData]
  type Api = Ingestion

  def version = 1

  def initialPerHashState(hash: Hash): Vector[IngestionData] = Vector.empty
  def processEvent(hash: Hash, value: Vector[IngestionData], event: MetadataEnvelope): Vector[IngestionData] =
    event.entry match {
      case entry if entry.kind == IngestionData => value :+ entry.value.asInstanceOf[IngestionData]
      case _                                    => value
    }

  def hasWork(hash: Hash, state: Vector[IngestionData]): Boolean = false
  def createWork(hash: Hash, state: Vector[IngestionData], context: ExtractionContext): (Vector[IngestionData], Vector[WorkEntry]) = (state, Vector.empty)

  def api(handleWithState: PerHashHandleWithStateFunc[PerHashState])(implicit ec: ExecutionContext): Ingestion = new Ingestion {
    def ingestionDataSink: Sink[(Hash, IngestionData), Any] =
      Flow[(Hash, IngestionData)]
        .map { case (hash, data) => (hash, handleNewEntry(hash, data)) }
        .to(handleWithState.handleStream)

    def ingest(hash: Hash, newData: IngestionData): Unit =
      handleWithState(hash) { state =>
        val (newState, ses) = handleNewEntry(hash, newData)(state)
        (newState, ses, ())
      }

    private def handleNewEntry(hash: Hash, newData: IngestionData): PerHashState => (PerHashState, Vector[WorkEntry]) = state => {
      def matches(data: IngestionData): Boolean =
        newData.originalFullFilePath == data.originalFullFilePath

      val newEntries =
        if (!state.exists(matches)) {
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

  import spray.json.DefaultJsonProtocol._
  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[Vector[IngestionData]] = implicitly[JsonFormat[Vector[IngestionData]]]
}