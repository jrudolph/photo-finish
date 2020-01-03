package net.virtualvoid.fotofinish

import akka.actor.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{ MergeHub, Sink, Source }
import net.virtualvoid.fotofinish.MetadataProcess.{ Journal, SideEffect }
import net.virtualvoid.fotofinish.metadata.{ Id, IngestionData, Metadata }

import scala.collection.immutable.TreeSet
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Success

trait MetadataApp {
  implicit def system: ActorSystem
  implicit def executionContext: ExecutionContext
  def config: RepositoryConfig

  def journal: Journal
  def ingestionDataSink: Sink[(Hash, IngestionData), Any]
  def ingest(hash: Hash, data: IngestionData): Unit
  def metadataFor(id: Id): Future[Metadata]
  def knownObjects(): Future[TreeSet[Id]]
  def completeIdPrefix(prefix: Id): Future[Option[Id]]
}

object MetadataApp {
  def apply(_config: RepositoryConfig)(implicit _system: ActorSystem): MetadataApp =
    new MetadataApp {
      def system: ActorSystem = _system
      implicit def executionContext: ExecutionContext = system.dispatcher
      val extractionContext = system.dispatchers.lookup("extraction-dispatcher")

      def config: RepositoryConfig = _config

      val journal = MetadataProcess.journal(Settings.manager)
      system.registerOnTermination(journal.shutdown())

      val executor: Sink[SideEffect, Any] =
        MergeHub.source[SideEffect]
          .mapAsyncUnordered(config.executorParallelism)(_().transform(Success(_)))
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
        MetadataProcess.asSource(process, Settings.manager, journal, extractionContext)
          .recoverWithRetries(1, {
            case ex =>
              println(s"Process [${process.id}] failed with [$ex]")
              ex.printStackTrace
              Source.empty
          })
          .to(executor)
          .run()

      val ingestor = runProcess(IngestionController)
      val metadataAccess = runProcess(PerObjectMetadataCollector)
      config.autoExtractors.foreach(e => runProcess(new MetadataIsCurrentProcess(e)))

      def ingestionDataSink: Sink[(Hash, IngestionData), Any] = ingestor.ingestionDataSink
      def ingest(hash: Hash, data: IngestionData): Unit = ingestor.ingest(hash, data)
      def metadataFor(id: Id): Future[Metadata] = metadataAccess.metadataFor(id)
      def knownObjects(): Future[TreeSet[Id]] = metadataAccess.knownObjects()

      def completeIdPrefix(prefix: Id): Future[Option[Id]] =
        knownObjects()
          .map { ids =>
            ids
              .from(prefix)
              .headOption
              .filter(_.idString startsWith prefix.idString)
          }
    }
}
