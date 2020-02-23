package net.virtualvoid.fotofinish

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ MergeHub, Sink, Source }
import net.virtualvoid.fotofinish.metadata.{ Id, IngestionData, Metadata }
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
  def faceApi: SimilarFaces
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

      import _config.entryFormat
      val ingestor = runProcess(IngestionController.toProcessSqlite)
      val metadataAccess = runProcess(PerObjectMetadataCollector.toProcessSqlite)
      val metadataStatuses = config.autoExtractors.toSeq.map(e => e -> runProcess(new MetadataIsCurrentProcess(e).toProcessSqlite))

      def ingestionDataSink: Sink[(Hash, IngestionData), Any] = ingestor.ingestionDataSink
      def ingest(hash: Hash, data: IngestionData): Unit = ingestor.ingest(hash, data)
      def metadataFor(id: Id): Future[Metadata] = metadataAccess.metadataFor(id)
      private lazy val _knownObjects = metadataAccess.knownObjects() // FIXME: should we cache it only once? Or reload regularly?
      def knownObjects(): Future[TreeSet[Id]] = _knownObjects
      def extractorStatus(): Future[Seq[(String, Map[String, Int])]] =
        Future.traverse(metadataStatuses) { case (extractor, a) => a.workHistogram.map(histo => extractor.kind -> histo) }

      val byOriginalFileName = runProcess(new HierarchySorter(OriginalFileNameHierarchy))
      val byYearMonth = runProcess(new HierarchySorter(YearMonthHierarchy))

      /*val faceApi = new SimilarFaces {
        override def similarFacesTo(hash: Hash, idx: Int): Future[Vector[(Hash, Int, Float)]] = Future.successful(Vector.empty)
      }*/
      val faceApi = runProcess(new PerFaceDistanceCollector(0.45f).toProcessSqlite)

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
