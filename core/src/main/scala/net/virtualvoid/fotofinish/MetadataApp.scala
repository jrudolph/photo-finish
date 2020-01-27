package net.virtualvoid.fotofinish

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ MergeHub, Sink, Source }
import net.virtualvoid.fotofinish.metadata.{ Id, IngestionData, Metadata, MetadataExtractor }
import net.virtualvoid.fotofinish.process._

import scala.collection.immutable.TreeSet
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Success

trait MetadataApp {
  implicit def system: ActorSystem
  implicit def executionContext: ExecutionContext
  def config: RepositoryConfig

  def journal: MetadataJournal
  def ingestionDataSink: Sink[(Hash, IngestionData), Any]
  def ingest(hash: Hash, data: IngestionData): Unit
  def metadataFor(id: Id): Future[Metadata]
  def knownObjects(): Future[TreeSet[Id]]
  def completeIdPrefix(prefix: Id): Future[Option[Id]]
  def extractorStatus(): Future[Seq[(String, Map[String, Int])]]
}

object MetadataApp {
  def apply(_config: RepositoryConfig)(implicit _system: ActorSystem): MetadataApp =
    new MetadataApp {
      def system: ActorSystem = _system
      implicit def executionContext: ExecutionContext = system.dispatcher
      val extractionContext = system.dispatchers.lookup("extraction-dispatcher")

      def config: RepositoryConfig = _config

      val journal = MetadataJournal(_config)
      system.registerOnTermination(journal.shutdown())

      val executor: Sink[WorkEntry, Any] =
        MergeHub.source[WorkEntry]
          .mapAsyncUnordered(config.executorParallelism)(_.run().transform(Success(_)))
          .mapConcat(_.toOption.toVector.flatten)
          .watchTermination() { (mat, fut) =>
            fut.onComplete { res =>
              println(s"Executor stopped with [$res]")
            }
            mat
          }
          .to(journal.newEntrySink)
          .run()

      def runProcess(process: MetadataProcess): process.Api =
        MetadataProcess.asSource(process, _config, journal, extractionContext)
          .recoverWithRetries(1, {
            case ex =>
              println(s"Process [${process.id}] failed with [$ex]")
              ex.printStackTrace
              Source.empty
          })
          .to(executor)
          .run()

      val ingestor = runProcess(PerHashIngestionController.toProcess)
      val metadataAccess = runProcess(PerObjectMetadataCollector.toProcess)
      val metadataStatuses = config.autoExtractors.toSeq.map(e => e -> runProcess(new PerHashMetadataIsCurrentProcess(e).toProcess))

      def ingestionDataSink: Sink[(Hash, IngestionData), Any] = ingestor.ingestionDataSink
      def ingest(hash: Hash, data: IngestionData): Unit = ingestor.ingest(hash, data)
      def metadataFor(id: Id): Future[Metadata] = metadataAccess.metadataFor(id)
      def knownObjects(): Future[TreeSet[Id]] = metadataAccess.knownObjects()
      def extractorStatus(): Future[Seq[(String, Map[String, Int])]] =
        Future.traverse(metadataStatuses) { case (extractor, a) => a.workHistogram.map(histo => extractor.kind -> histo) }

      def completeIdPrefix(prefix: Id): Future[Option[Id]] =
        knownObjects()
          .map { ids =>
            ids
              .rangeFrom(prefix)
              .headOption
              .filter(_.idString startsWith prefix.idString)
          }
    }
}
