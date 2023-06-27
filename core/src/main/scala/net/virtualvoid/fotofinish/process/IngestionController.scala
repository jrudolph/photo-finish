package net.virtualvoid.fotofinish.process

import org.apache.pekko.http.scaladsl.model.DateTime
import org.apache.pekko.stream.scaladsl.{ Flow, Sink }
import net.virtualvoid.fotofinish.Hash
import net.virtualvoid.fotofinish.metadata.Id.Hashed
import net.virtualvoid.fotofinish.metadata.{ CreationInfo, Id, Ingestion, IngestionData, MetadataEntry, MetadataEnvelope }
import spray.json.JsonFormat

import scala.concurrent.{ ExecutionContext, Future }

trait Ingestion {
  def ingestionDataSink: Sink[(Hash, IngestionData), Any]
  def ingest(hash: Hash, data: IngestionData): Unit
}

object IngestionController extends PerIdProcessWithNoGlobalState {
  type PerKeyState = Vector[IngestionData]
  type Api = Ingestion

  def version = 1

  def initialPerKeyState(id: Id): Vector[IngestionData] = Vector.empty
  def processIdEvent(id: Id, event: MetadataEnvelope): Effect =
    Effect.mapKeyState(id) { value =>
      event.entry match {
        case entry if entry.kind == IngestionData => value :+ entry.value.asInstanceOf[IngestionData]
        case _                                    => value
      }
    }

  def api(handleWithState: AccessStateFunc)(implicit ec: ExecutionContext): Ingestion = new Ingestion {
    def ingestionDataSink: Sink[(Hash, IngestionData), Any] =
      Flow[(Hash, IngestionData)]
        .map { case (hash, data) => (Hashed(hash), handleNewEntry(hash, data)) }
        .to(handleWithState.handleStream)

    def ingest(hash: Hash, newData: IngestionData): Unit =
      handleWithState(Hashed(hash)) { state =>
        val (newState, ses) = handleNewEntry(hash, newData)(state)
        (newState, ses, ())
      }

    private def handleNewEntry(hash: Hash, newData: IngestionData): PerKeyState => (PerKeyState, Vector[WorkEntry]) = state => {
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
            CreationInfo(DateTime.now, inferred = false, Ingestion),
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