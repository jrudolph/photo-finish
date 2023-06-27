package net.virtualvoid.fotofinish

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.DateTime
import org.apache.pekko.stream.scaladsl.{ MergeHub, Sink, Source }
import net.virtualvoid.fotofinish.metadata.{ ExtractionContext, FaceData, Id, IngestionData, Metadata, MetadataEntry, MetadataKind }
import net.virtualvoid.fotofinish.process._
import spray.json.JsonFormat

import java.io.File
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
  def phashApi: SimilarImages

  def mostRecentlyFoundFaces(): Future[Seq[(Hash, Int, DateTime)]]

  def byYearMonth: HierarchyAccess[String]
  def byOriginalFileName: HierarchyAccess[String]

  def aggregation[S: JsonFormat](id: String, version: Int, kind: MetadataKind, initial: S)(f: (S, kind.T) => S): () => Future[S]
}

object MetadataApp {
  def apply(_config: RepositoryConfig)(implicit _system: ActorSystem): MetadataApp =
    new MetadataApp {
      val processConfig = _config: ProcessConfig
      import processConfig.entryFormat

      def system: ActorSystem = _system
      implicit def executionContext: ExecutionContext = system.dispatcher
      val extractionContext = new ExtractionContext {
        val executionContext: ExecutionContext = system.dispatchers.lookup("extraction-dispatcher")
        def accessData[T](id: Id)(f: File => Future[T]): Future[T] =
          // FIXME: easy for now as we expect all hashes to be available as files
          f(config.repoFileFor(id.hash))
      }

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
        MetadataProcess.asSource(process, _config, journal)
          .recoverWithRetries(1, {
            case ex =>
              println(s"Process [${process.id}] failed with [$ex]")
              ex.printStackTrace
              Source.empty
          })
          .to(executor)
          .run()

      val ingestor = runProcess(IngestionController.toProcessSqlite)
      val metadataAccess = runProcess(PerObjectMetadataCollector.toProcessSqlite)
      val metadataStatuses = config.autoExtractors.toSeq.map(e => e -> runProcess(new MetadataIsCurrentProcess(e, extractionContext).toProcessSqlite))

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
      val phashApi = runProcess(new PHashDistanceCollector(8).toProcessSqlite)

      def completeIdPrefix(prefix: Id): Future[Option[Id]] =
        knownObjects()
          .map { ids =>
            ids
              .rangeFrom(prefix)
              .headOption
              .filter(_.idString startsWith prefix.idString)
          }

      def aggregation[S: JsonFormat](id: String, version: Int, kind: MetadataKind, initial: S)(f: (S, kind.T) => S): () => Future[S] =
        aggregationWithEntry(id, version, kind, initial)((state, entry) => f(state, entry.value))
      def aggregationWithEntry[S: JsonFormat](id: String, version: Int, kind: MetadataKind, initial: S)(f: (S, MetadataEntry.Aux[kind.T]) => S): () => Future[S] =
        runProcess(new SimpleAggregationProcess[S, kind.T](id, version, kind, initial, f))

      val mostRecentlyFoundFacesProcess: () => Future[Seq[(Hash, Int, DateTime)]] = {
        val max = 50
        type RecentFaceEntry = (Hash, Int, DateTime)
        import spray.json.DefaultJsonProtocol._
        import net.virtualvoid.fotofinish.metadata.MetadataJsonProtocol.dateTimeFormat

        aggregationWithEntry("recently-found-faces", 1, FaceData, Vector.empty[RecentFaceEntry]) { (recent, entry) =>
          val newFaces = entry.value.faces.indices.map { i =>
            (entry.target.hash, i, entry.creation.created)
          }
          (recent ++ newFaces).sortBy(-_._3.clicks).take(max)
        }
      }

      override def mostRecentlyFoundFaces(): Future[Seq[(Hash, Int, DateTime)]] = mostRecentlyFoundFacesProcess()
    }
}
