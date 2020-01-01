package net.virtualvoid.fotofinish

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ MergeHub, Sink, Source }
import net.virtualvoid.fotofinish.MetadataProcess.{ Journal, SideEffect }
import net.virtualvoid.fotofinish.metadata.{ Id, Metadata }

import scala.collection.immutable.TreeSet
import scala.concurrent.{ ExecutionContext, Future }

trait MetadataApp {
  implicit def system: ActorSystem
  implicit def executionContext: ExecutionContext
  def config: RepositoryConfig

  def journal: Journal
  def ingest(fileInfo: FileInfo): Unit
  def metadataFor(id: Id): Future[Metadata]
  def knownObjects(): Future[TreeSet[Id]]
  def completeIdPrefix(prefix: Id): Future[Option[Id]]
}

object MetadataApp {
  def apply(_config: RepositoryConfig)(implicit _system: ActorSystem): MetadataApp =
    new MetadataApp {
      def system: ActorSystem = _system
      implicit def executionContext: ExecutionContext = system.dispatcher

      def config: RepositoryConfig = _config

      val journal = MetadataProcess.journal(Settings.manager)
      system.registerOnTermination(journal.shutdown())

      val executor: Sink[SideEffect, Any] =
        MergeHub.source[SideEffect]
          .mapAsyncUnordered(config.executorParallelism)(_())
          .mapConcat(identity)
          .to(journal.newEntrySink)
          .run()

      def runProcess(process: MetadataProcess): process.Api =
        MetadataProcess.asSource(process, Settings.manager, journal)
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

      def ingest(fileInfo: FileInfo): Unit = ingestor(fileInfo)
      def metadataFor(id: Id): Future[Metadata] = metadataAccess.metadataFor(id)
      def knownObjects(): Future[TreeSet[Id]] = metadataAccess.knownObjects()

      /*def completeHash(hashPrefix: String): Future[Option[Id]] =
        metadataAccess.knownObjects()
          .map(_.find(_.hash.asHexString.startsWith()))*/
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
