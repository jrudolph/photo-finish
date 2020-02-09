package net.virtualvoid.fotofinish
package process

import java.io.{ File, FileOutputStream }
import java.nio.file.{ Files, StandardCopyOption }
import java.sql.{ Connection, DriverManager, ResultSet }
import java.util.zip.GZIPOutputStream

import akka.actor.ActorSystem
import akka.http.scaladsl.model.DateTime
import akka.stream._
import akka.stream.scaladsl.{ Compression, FileIO, Flow, Framing, Keep, MergeHub, Sink, Source }
import akka.util.ByteString
import javax.sql.DataSource
import net.virtualvoid.fotofinish.metadata.Id.Hashed
import net.virtualvoid.fotofinish.metadata._
import net.virtualvoid.fotofinish.util.{ JsonExtra, StatefulDetachedFlow }
import spray.json.JsonFormat

import scala.collection.immutable.TreeSet
import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

trait HandleWithStateFunc[S] {
  def apply[T](f: S => (S, Vector[WorkEntry], T)): Future[T]
  def access[T](f: S => T): Future[T] =
    apply { state =>
      (state, Vector.empty, f(state))
    }

  def handleStream: Sink[S => (S, Vector[WorkEntry]), Any]
}

case class Snapshot[S](processId: String, processVersion: Int, currentSeqNr: Long, state: S)

trait MetadataProcess {
  type S
  type Api

  def id: String = getClass.getName
  def version: Int

  def initialState: S
  def processEvent(state: S, event: MetadataEnvelope): S
  def createWork(state: S, context: ExtractionContext): (S, Vector[WorkEntry])
  def api(handleWithState: HandleWithStateFunc[S])(implicit ec: ExecutionContext): Api

  /** Allows to prepare state loaded from snapshot */
  def initializeStateSnapshot(state: S): S = state

  def saveSnapshot(target: File, config: RepositoryConfig, snapshot: Snapshot[S]): S
  def loadSnapshot(target: File, config: RepositoryConfig)(implicit system: ActorSystem): Option[Snapshot[S]]
}
trait LineBasedJsonSnaphotProcess extends MetadataProcess {
  import spray.json.DefaultJsonProtocol._
  import spray.json._

  private case class SnapshotHeader(processId: String, processVersion: Int, currentSeqNr: Long)
  private implicit val headerFormat = jsonFormat3(SnapshotHeader.apply)
  def saveSnapshot(targetFile: File, config: RepositoryConfig, snapshot: Snapshot[S]): S = {
    val tmpFile = File.createTempFile(targetFile.getName, ".tmp", targetFile.getParentFile)
    val os = new GZIPOutputStream(new FileOutputStream(tmpFile))

    try {
      implicit val seFormat = stateEntryFormat(config.entryFormat)
      os.write(SnapshotHeader(snapshot.processId, snapshot.processVersion, snapshot.currentSeqNr).toJson.compactPrint.getBytes("utf8"))
      os.write('\n')
      stateAsEntries(snapshot.state).foreach { entry =>
        os.write(entry.toJson.compactPrint.getBytes("utf8"))
        os.write('\n')
      }
    } finally os.close()
    Files.move(tmpFile.toPath, targetFile.toPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
    snapshot.state
  }
  def loadSnapshot(file: File, config: RepositoryConfig)(implicit system: ActorSystem): Option[Snapshot[S]] = {
    import system.dispatcher

    if (file.exists) {
      implicit val seFormat: JsonFormat[StateEntryT] = stateEntryFormat(config.entryFormat)

      val headerAndEntriesF =
        FileIO.fromPath(file.toPath)
          .via(Compression.gunzip())
          .via(Framing.delimiter(ByteString("\n"), 1000000000))
          .map(_.utf8String)
          .prefixAndTail(1)
          .runWith(Sink.head)
          .flatMap {
            case (Seq(header), entries) =>
              val h = header.parseJson.convertTo[SnapshotHeader]
              entries
                .map(_.parseJson.convertTo[StateEntryT])
                .runWith(Sink.seq)
                .map(es =>
                  Snapshot[S](h.processId, h.processVersion, h.currentSeqNr, entriesAsState(es))
                )
          }

      // FIXME
      val hs = Await.ready(headerAndEntriesF, 60.seconds)

      hs.value.get
        .map { s =>
          require(s.processId == id, s"Unexpected process ID in snapshot [${s.processId}]. Expected [$id].")
          require(s.processVersion == version, s"Wrong version in snapshot [${s.processVersion}]. Expected [$version].")

          Some(s.copy(state = initializeStateSnapshot(s.state)))
        }
        .recover {
          case NonFatal(ex) =>
            println(s"Reading snapshot failed because of ${ex.getMessage}. Discarding snapshot.")
            ex.printStackTrace()
            None
        }
        .get
    } else
      None
  }

  type StateEntryT
  def stateAsEntries(state: S): Iterator[StateEntryT]
  def entriesAsState(entries: Iterable[StateEntryT]): S
  def stateEntryFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[StateEntryT]
}
trait SingleEntryState extends LineBasedJsonSnaphotProcess {
  override type StateEntryT = S

  def stateAsEntries(state: S): Iterator[S] = Iterator(state)
  def entriesAsState(entries: Iterable[S]): S = entries.head
  def stateEntryFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[S] = stateFormat
  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[S]
}

object MetadataProcess {
  sealed trait StreamEntry
  final case class Metadata(entry: MetadataEnvelope) extends StreamEntry
  case object AllObjectsReplayed extends StreamEntry
  case object MakeSnapshot extends StreamEntry
  case object ShuttingDown extends StreamEntry
  final case class Execute[S, T](f: S => (S, Vector[WorkEntry], T), promise: Promise[T]) extends StreamEntry

  def asSource(p: MetadataProcess, config: RepositoryConfig, journal: MetadataJournal, extractionEC: ExecutionContext)(implicit system: ActorSystem, ec: ExecutionContext): Source[WorkEntry, p.Api] = {
    val injectApi: Source[StreamEntry, p.Api] =
      Source.queue[StreamEntry](10000, OverflowStrategy.dropNew) // FIXME: will this be enough for mass injections?
        .mergeMat(MergeHub.source[StreamEntry])(Keep.both)
        .mapMaterializedValue {
          case (queue, sink) =>
            p.api(new HandleWithStateFunc[p.S] {
              def apply[T](f: p.S => (p.S, Vector[WorkEntry], T)): Future[T] = {
                val promise = Promise[T]
                queue.offer(Execute(f, promise))
                  .onComplete {
                    case Success(QueueOfferResult.Enqueued) =>
                    case x                                  => println(s"Injecting entries failed because of $x")
                  }
                promise.future
              }

              def handleStream: Sink[p.S => (p.S, Vector[WorkEntry]), Any] =
                Flow[p.S => (p.S, Vector[WorkEntry])]
                  .map { f =>
                    Execute[p.S, Unit]({ state =>
                      val (newState, ses) = f(state)
                      (newState, ses, ())
                    }, Promise.successful(()))
                  }
                  .to(sink)
            })
        }

    val extractionContext = new ExtractionContext {
      def executionContext: ExecutionContext = extractionEC
      def accessData[T](hash: Hash)(f: File => Future[T]): Future[T] =
        // FIXME: easy for now as we expect all hashes to be available as files
        f(config.fileInfoOf(hash).repoFile)
    }

    type Handler = ProcessState => StreamEntry => ProcessState
    case class ProcessState(seqNr: Long, processState: p.S, handler: Handler, workEntries: Vector[WorkEntry], lastSnapshotAt: Long, hasFinishedReplaying: Boolean, finished: Boolean) {
      def run(streamEntry: StreamEntry): ProcessState = handler(this)(streamEntry)

      def runEntry(envelope: MetadataEnvelope): ProcessState = copy(processState = p.processEvent(processState, envelope))

      def execute[T](e: Execute[p.S, T]): ProcessState =
        Try(e.f(processState)) match {
          case Success((newState, workEntries, res)) =>
            e.promise.trySuccess(res)
            this
              .withState(newState)
              .addWorkEntries(workEntries)
          case Failure(ex) =>
            e.promise.tryFailure(ex)
            this
        }

      def withSeqNr(newSeqNr: Long): ProcessState = copy(seqNr = newSeqNr)
      def withHandler(newHandler: Handler): ProcessState = copy(handler = newHandler)
      def withState(newState: p.S): ProcessState = copy(processState = newState)
      def withLastSnapshotAt(newValue: Long): ProcessState = copy(lastSnapshotAt = newValue)
      def withLastSnapshotNow: ProcessState = copy(lastSnapshotAt = seqNr)
      def addWorkEntries(newWorkEntries: Vector[WorkEntry]): ProcessState = copy(workEntries = workEntries ++ newWorkEntries)
      def clearWorkEntries: ProcessState = copy(workEntries = Vector.empty)

      def emit: (ProcessState, Vector[WorkEntry]) =
        if (hasFinishedReplaying)
          if (workEntries.nonEmpty) (clearWorkEntries, workEntries)
          else {
            val (newState, workEntries) = p.createWork(processState, extractionContext)
            (withState(newState), workEntries)
          }
        else
          (this, Vector.empty)

      def setFinishedReplaying: ProcessState = copy(hasFinishedReplaying = true)
      def setFinished: ProcessState = copy(finished = true)

      def saveSnapshot(force: Boolean = false): ProcessState =
        if ((lastSnapshotAt + config.snapshotOffset < seqNr) || (force && lastSnapshotAt < seqNr)) bench("Serializing state") {
          val newState = p.saveSnapshot(processSnapshotFile(p, config), config, Snapshot(p.id, p.version, seqNr, processState))
          this
            .withLastSnapshotNow
            .withState(newState)
        }
        else this
    }
    def bench[T](what: String)(f: => T): T = {
      val started = System.nanoTime()
      val res = f
      val lasted = System.nanoTime() - started
      println(s"[${p.id}] $what in ${lasted / 1000000} ms")
      res
    }
    def replaying(waitingExecutions: Vector[Execute[p.S, _]]): Handler = state => {
      case Metadata(e) =>
        if (e.seqNr == state.seqNr + 1) {
          state
            .runEntry(e)
            .withSeqNr(e.seqNr)
        } else if (e.seqNr < state.seqNr + 1)
          throw new IllegalStateException(s"Got unexpected duplicate seqNr ${e.seqNr} after last ${state.seqNr}")
        else
          throw new IllegalStateException(s"Got unexpected gap in seqNr ${e.seqNr} after last ${state.seqNr}")

      case AllObjectsReplayed =>
        println(s"[${p.id}] finished replaying after [${state.seqNr}]")

        // run all waiting executions
        waitingExecutions.foldLeft(state)(_.execute(_))
          .saveSnapshot()
          .setFinishedReplaying
          .withHandler(ignoreDuplicateSeqNrs)

      case e: Execute[p.S, t] @unchecked =>
        state.withHandler(replaying(waitingExecutions :+ e))

      case MakeSnapshot => state.saveSnapshot()
      case ShuttingDown => state.saveSnapshot(force = true).setFinished
    }
    def ignoreDuplicateSeqNrs: Handler = state => {
      case Metadata(e) if e.seqNr <= state.seqNr =>
        println(s"[${p.id}] at ${state.seqNr} got ${e.seqNr}. Ignoring")
        state
      case m: Metadata =>
        state
          .withHandler(liveEvents)
          .run(m)

      case e: Execute[p.S, t] @unchecked => state.execute(e)

      case MakeSnapshot                  => state.saveSnapshot()
      case ShuttingDown                  => state.saveSnapshot(force = true).setFinished
    }
    def liveEvents: Handler = state => {
      case Metadata(e) =>
        if (e.seqNr == state.seqNr + 1) {
          state
            .runEntry(e)
            .withSeqNr(e.seqNr)
        } else if (e.seqNr < state.seqNr + 1)
          throw new IllegalStateException(s"Got unexpected duplicate seqNr ${e.seqNr} after last ${state.seqNr}, ignoring...")
        else
          throw new IllegalStateException(s"Got unexpected gap in seqNr ${e.seqNr} after last ${state.seqNr}")

      case e: Execute[p.S, t] @unchecked => state.execute(e)

      case MakeSnapshot                  => state.saveSnapshot()
      case ShuttingDown                  => state.saveSnapshot(force = true).setFinished

      case AllObjectsReplayed            => throw new IllegalStateException("Unexpected AllObjectsReplayed in state liveEvents")
    }

    def statefulDetachedFlow[T, U, S](initialState: () => S, handle: (S, T) => S, emit: S => (S, Vector[U]), isFinished: S => Boolean): Flow[T, U, Any] =
      Flow.fromGraph(new StatefulDetachedFlow(initialState, handle, emit, isFinished))

    val snapshot = bench("Deserializing state")(p.loadSnapshot(processSnapshotFile(p, config), config).getOrElse(Snapshot(p.id, p.version, -1L, p.initialState)))
    println(s"[${p.id}] initialized process at seqNr [${snapshot.currentSeqNr}]")

    val processFlow =
      Flow[StreamEntry]
        .merge(Source.tick(config.snapshotInterval, config.snapshotInterval, MakeSnapshot))
        .mergeMat(injectApi)(Keep.right)
        .via(statefulDetachedFlow[StreamEntry, WorkEntry, ProcessState](
          () => ProcessState(snapshot.currentSeqNr, snapshot.state, replaying(Vector.empty), Vector.empty, lastSnapshotAt = snapshot.currentSeqNr, hasFinishedReplaying = false, finished = false),
          _.run(_),
          _.emit,
          _.finished
        ))
        .recoverWithRetries(1, {
          case ex =>
            println(s"Process for [${p.id}] failed with [${ex.getMessage}]. Aborting the process.")
            ex.printStackTrace()
            Source.empty
        })

    journal.source(snapshot.currentSeqNr + 1)
      .map {
        case m @ Metadata(entry) =>
          if ((entry.seqNr % 100) == 0) println(s"[${p.id}] at [${entry.seqNr}]")

          m
        case e =>
          println(s"[${p.id}] at [$e]")
          e
      }
      .viaMat(processFlow)(Keep.right)
  }

  // FIXME: join with header
  private def processSnapshotFile(p: MetadataProcess, config: RepositoryConfig): File =
    new File(config.metadataDir, s"${p.id.replaceAll("""[\[\]]""", "_")}.snapshot")
}

object GetAllObjectsProcess extends MetadataProcess with SingleEntryState {
  case class State(knownHashes: Set[Hash])
  type S = State
  type Api = () => Future[Set[Hash]]

  def version: Int = 1

  def initialState: State = State(Set.empty)
  def processEvent(state: State, event: MetadataEnvelope): State =
    state.copy(knownHashes = state.knownHashes + event.entry.target.hash)
  def createWork(state: State, context: ExtractionContext): (State, Vector[WorkEntry]) = (state, Vector.empty)

  def api(handleWithState: HandleWithStateFunc[S])(implicit ec: ExecutionContext): Api =
    () => handleWithState { state =>
      (state, Vector.empty, state.knownHashes)
    }

  import spray.json.DefaultJsonProtocol._
  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[State] = jsonFormat1(State.apply)
}

trait PerHashHandleWithStateFunc[S] {
  def apply[T](key: Hash)(f: S => (S, Vector[WorkEntry], T)): Future[T]
  def access[T](key: Hash)(f: S => T): Future[T] =
    apply(key) { state =>
      (state, Vector.empty, f(state))
    }

  def accessAll[T](f: Iterator[(Hash, S)] => T): Future[T]
  def accessAllKeys[T](f: Iterator[Hash] => T): Future[T]

  def handleStream: Sink[(Hash, S => (S, Vector[WorkEntry])), Any]
}
trait ConnectionProvider {
  def apply[T](f: Connection => T): T
}
object ConnectionProvider {
  def fromDataSource(ds: DataSource): ConnectionProvider =
    new ConnectionProvider {
      override def apply[T](f: Connection => T): T = {
        val conn = ds.getConnection
        try f(conn)
        finally conn.close()
      }
    }
}

trait SimplePerHashProcess { php =>
  type PerHashState
  type Api

  def id: String = getClass.getName
  def version: Int

  def initialPerHashState(hash: Hash): PerHashState
  def processEvent(hash: Hash, state: PerHashState, event: MetadataEnvelope): PerHashState
  def hasWork(hash: Hash, state: PerHashState): Boolean
  def createWork(hash: Hash, state: PerHashState, context: ExtractionContext): (PerHashState, Vector[WorkEntry])
  def api(handleWithState: PerHashHandleWithStateFunc[PerHashState])(implicit ec: ExecutionContext): Api

  /** Allows to prepare state loaded from snapshot */
  def initializeStateSnapshot(hash: Hash, state: PerHashState): PerHashState = state

  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[PerHashState]

  def toProcess: MetadataProcess { type Api = php.Api } =
    new LineBasedJsonSnaphotProcess {
      case class State(data: Map[Hash, PerHashState]) {
        def update[T](hash: Hash)(f: PerHashState => PerHashState): State =
          copy(data = data.updated(hash, f(get(hash))))
        def get(hash: Hash): PerHashState = data.getOrElse(hash, initialPerHashState(hash))
      }

      type S = State
      type Api = php.Api
      override def id: String = php.id
      def version: Int = php.version

      def initialState: S = State(Map.empty)
      def processEvent(state: State, event: MetadataEnvelope): State = {
        val hash = event.entry.target.hash
        state.update(hash)(s => php.processEvent(hash, s, event))
      }

      def createWork(state: S, context: ExtractionContext): (S, Vector[WorkEntry]) = {
        state.data
          .iterator
          .filter(d => php.createWork(d._1, d._2, context)._2.nonEmpty)
          .take(10) // FIXME: make parameterizable?
          .map {
            case (hash, phs) =>
              val (newState, work) = php.createWork(hash, phs, context)
              ((s: State) => s.update(hash)(_ => newState), work)
          }
          .foldLeft((state, Vector.empty[WorkEntry])) { (cur, next) =>
            val (s, wes) = cur
            val (f, newWes) = next
            (f(s), wes ++ newWes)
          }
      }

      def api(handleWithState: HandleWithStateFunc[S])(implicit ec: ExecutionContext): Api =
        php.api(new PerHashHandleWithStateFunc[PerHashState] {
          override def apply[T](key: Hash)(f: PerHashState => (PerHashState, Vector[WorkEntry], T)): Future[T] =
            handleWithState.apply[T] { state =>
              val (newState, entries, t) = f(state.get(key))
              (state.update(key)(_ => newState), entries, t)
            }

          override def handleStream: Sink[(Hash, PerHashState => (PerHashState, Vector[WorkEntry])), Any] =
            Flow[(Hash, PerHashState => (PerHashState, Vector[WorkEntry]))]
              .map[State => (State, Vector[WorkEntry])] {
                case (hash, f) => state =>
                  val (newState, entries) = f(state.get(hash))
                  (state.update(hash)(_ => newState), entries)
              }
              .to(handleWithState.handleStream)

          override def accessAll[T](f: Iterator[(Hash, PerHashState)] => T): Future[T] =
            handleWithState.access { state => f(state.data.iterator) }

          override def accessAllKeys[T](f: Iterator[Hash] => T): Future[T] =
            handleWithState.access { state => f(state.data.keys.iterator) }
        })

      override def initializeStateSnapshot(state: State): State =
        // FIXME: this can be quite expensive, we should probably have some global state to track state that needs to be
        // fixed after restart
        State(state.data.map { case (k, v) => k -> php.initializeStateSnapshot(k, v) })

      type StateEntryT = (Hash, PerHashState)
      def stateAsEntries(state: State): Iterator[(Hash, PerHashState)] = state.data.iterator
      def entriesAsState(entries: Iterable[(Hash, PerHashState)]): State = State(entries.toMap)
      def stateEntryFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[(Hash, PerHashState)] = {
        import spray.json.DefaultJsonProtocol._
        implicit val phsFormat = php.stateFormat
        implicitly[JsonFormat[(Hash, PerHashState)]]
      }
    }

  def toProcessSqlite(implicit entryFormat: JsonFormat[MetadataEntry]): MetadataProcess { type Api = php.Api } =
    new MetadataProcess {
      type S = State
      type Api = php.Api
      override def id: String = php.id
      def version: Int = php.version

      case class State(
          cachedData: Map[Hash, PerHashState],
          dirty:      Set[Hash],
          newEntries: Set[Hash],
          withWork:   Set[Hash],
          connection: Option[Connection]) {
        def update(hash: Hash)(f: PerHashState => PerHashState): State = {
          val existing = getIfExists(hash)
          val newState = f(existing.getOrElse(initialPerHashState(hash)))
          val newNew = if (existing.isDefined) newEntries else newEntries + hash
          val hasW = hasWork(hash, newState)
          val newWithWork =
            if (hasW) withWork + hash
            else withWork - hash
          copy(
            cachedData = cachedData + (hash -> newState),
            dirty = dirty + hash,
            newEntries = newNew,
            withWork = newWithWork
          ) //.flushIfNecessary()
        }
        def removeFromWorkList(hash: Hash): State = copy(withWork = withWork - hash)
        def getIfExists(hash: Hash): Option[PerHashState] =
          cachedData.get(hash).orElse(retrieve(hash))
        def get(hash: Hash): PerHashState =
          getIfExists(hash).getOrElse(initialPerHashState(hash))
        def retrieve(hash: Hash): Option[PerHashState] = connection.flatMap { conn =>
          val stmt = conn.prepareStatement("select data from per_hash_data where hash = ?")
          stmt.setString(1, hash.toString)
          val rs = stmt.executeQuery()

          if (rs.next()) Some(readFromRS(rs))
          else None
        }
        def data: Iterator[(Hash, PerHashState)] = {
          val existing =
            connection.iterator.flatMap { conn =>
              val rs =
                conn.createStatement()
                  .executeQuery("select hash, data from per_hash_data")
              Iterator.unfold(rs) { rs =>
                if (rs.next()) Some {
                  import spray.json._
                  implicit val phsFormat = php.stateFormat
                  val hash = Hash.fromPrefixedString(rs.getString("hash")).get

                  def load(): PerHashState = rs.getString("data").parseJson.convertTo[php.PerHashState]

                  ((hash, cachedData.getOrElse(hash, load())), rs)
                }
                else None
              }
            }
          val newE = newEntries.iterator.map(n => n -> cachedData(n))
          existing ++ newE
        }
        def keys: Iterator[Hash] = {
          val existing =
            connection.iterator.flatMap { conn =>
              val rs =
                conn.createStatement()
                  .executeQuery("select hash from per_hash_data")

              Iterator.unfold(rs) { rs =>
                if (rs.next()) Some {
                  (Hash.fromPrefixedString(rs.getString("hash")).get, rs)
                }
                else None
              }
            }
          existing ++ newEntries.iterator
        }

        private def readFromRS(rs: ResultSet): PerHashState = {
          import spray.json._
          implicit val phsFormat = php.stateFormat
          rs.getString("data").parseJson.convertTo[php.PerHashState]
        }

        def onlyDirty: State =
          copy(cachedData = cachedData.view.filterKeys(k => dirty(k) || newEntries(k)).toMap)
      }

      def initialState: S = State(Map.empty, Set.empty, Set.empty, Set.empty, None)
      def processEvent(state: State, event: MetadataEnvelope): State = {
        val hash = event.entry.target.hash
        state.update(hash)(s => php.processEvent(hash, s, event))
      }

      def createWork(state: S, context: ExtractionContext): (S, Vector[WorkEntry]) =
        state.withWork
          .take(10)
          .map(h => h -> state.get(h))
          .map {
            case (hash, phs) =>
              val (newState, work) = php.createWork(hash, phs, context)
              (
                (s: State) => s.update(hash)(_ => newState).removeFromWorkList(hash),
                work
              )
          }
          .foldLeft((state, Vector.empty[WorkEntry])) { (cur, next) =>
            val (s, wes) = cur
            val (f, newWes) = next
            (f(s), wes ++ newWes)
          }

      def api(handleWithState: HandleWithStateFunc[S])(implicit ec: ExecutionContext): Api =
        php.api(new PerHashHandleWithStateFunc[PerHashState] {
          override def apply[T](key: Hash)(f: PerHashState => (PerHashState, Vector[WorkEntry], T)): Future[T] =
            handleWithState.apply[T] { state =>
              val (newState, entries, t) = f(state.get(key))
              (state.update(key)(_ => newState), entries, t)
            }

          override def handleStream: Sink[(Hash, PerHashState => (PerHashState, Vector[WorkEntry])), Any] =
            Flow[(Hash, PerHashState => (PerHashState, Vector[WorkEntry]))]
              .map[State => (State, Vector[WorkEntry])] {
                case (hash, f) => state =>
                  val (newState, entries) = f(state.get(hash))
                  (state.update(hash)(_ => newState), entries)

              }
              .to(handleWithState.handleStream)

          override def accessAll[T](f: Iterator[(Hash, PerHashState)] => T): Future[T] =
            handleWithState.access(state => f(state.data))

          override def accessAllKeys[T](f: Iterator[Hash] => T): Future[T] =
            handleWithState.access(state => f(state.keys))
        })

      override def initializeStateSnapshot(state: State): State = state
      // FIXME: this can be quite expensive, we should probably have some global state to track state that needs to be
      // fixed after restart
      //State(state.data.map { case (k, v) => k -> php.initializeStateSnapshot(k, v) })
      //???

      def saveSnapshot(target: File, config: RepositoryConfig, snapshot: Snapshot[State]): State = {
        val state = snapshot.state
        val conn = state.connection.getOrElse(openConnectionTo(target))
        val stmt = conn.createStatement
        stmt.execute("begin transaction")
        stmt.execute(s"create table if not exists per_hash_data(hash PRIMARY KEY, data)")
        stmt.execute("create table if not exists meta(process_id, process_version, seq_nr)")
        stmt.execute("create table if not exists has_work(hash)")
        stmt.executeUpdate("delete from has_work")
        stmt.executeUpdate("delete from meta")

        val prepared = conn.prepareStatement("insert into meta(process_id, process_version, seq_nr) values(?, ?, ?)")
        prepared.setString(1, snapshot.processId)
        prepared.setInt(2, snapshot.processVersion)
        prepared.setLong(3, snapshot.currentSeqNr)
        prepared.executeUpdate() // TODO: check for result

        import spray.json._
        implicit val phsFormat = php.stateFormat

        val insert = conn.prepareStatement(s"insert or replace into per_hash_data(hash, data) values (?, ?)")
        val toFlush =
          state.dirty
            .map { hash =>
              hash.toString -> state.cachedData(hash).toJson.compactPrint
            }

        toFlush
          .foreach {
            case (hash, data) =>
              insert.setString(1, hash)
              insert.setString(2, data)
              insert.execute()
          }

        stmt.execute("commit transaction")
        state.copy(dirty = Set.empty, newEntries = Set.empty, connection = Some(conn)) // TODO: drop part of the cache?
      }
      def loadSnapshot(target: File, config: RepositoryConfig)(implicit system: ActorSystem): Option[Snapshot[State]] =
        if (target.exists()) {
          val conn = openConnectionTo(target)
          val stmt = conn.createStatement
          val rs = stmt.executeQuery("select process_id, process_version, seq_nr from meta")
          val processId = rs.getString("process_id")
          val processVersion = rs.getInt("process_version")
          val seqNr = rs.getLong("seq_nr")

          val rs2 = stmt.executeQuery("select hash from has_work")
          val hasWork = Iterator.unfold(rs2) { rs =>
            if (rs.next()) Some {
              (Hash.fromPrefixedString(rs.getString("hash")).get, rs)
            }
            else None
          }.toSet

          Some {
            Snapshot(
              processId,
              processVersion,
              seqNr,
              State(Map.empty, Set.empty, Set.empty, hasWork, Some(conn))
            )
          }
        } else None

      private def openConnectionTo(target: File): Connection = {
        val url = s"jdbc:sqlite:${target.getAbsolutePath}"
        DriverManager.getConnection(url)
      }
    }
}

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

trait MetadataExtractionScheduler {
  def workHistogram: Future[Map[String, Int]]
}

class PerHashMetadataIsCurrentProcess(extractor: MetadataExtractor) extends SimplePerHashProcess {
  type PerHashState = HashState

  sealed trait DependencyState {
    def exists: Boolean
    def get: MetadataEntry
  }
  case class Missing(kind: String, version: Int) extends DependencyState {
    def exists: Boolean = false
    def get: MetadataEntry = throw new IllegalStateException(s"Metadata for kind $kind was still missing")
  }
  case class Existing(value: MetadataEntry) extends DependencyState {
    def exists: Boolean = true
    def get: MetadataEntry = value
  }

  sealed trait HashState extends Product {
    def handle(entry: MetadataEntry): HashState
  }
  case class CollectingDependencies(dependencyState: Vector[DependencyState]) extends HashState {
    def handle(entry: MetadataEntry): HashState =
      if (entry.kind.kind == extractor.metadataKind.kind)
        if (entry.kind.version == extractor.metadataKind.version) LatestVersion
        else Calculated(entry.kind.version)
      else {
        val newL =
          dependencyState.map {
            case Missing(k, v) if k == entry.kind.kind && v == entry.kind.version => Existing(entry)
            case x => x // FIXME: handle dependency changes
          }

        if (newL forall (_.exists))
          extractor.precondition(entry.target.hash, newL.map(_.get)) match {
            case None        => Ready(newL.map(_.get))
            case Some(cause) => PreConditionNotMet(cause)
          }
        else CollectingDependencies(newL)
      }

    def hasAllDeps: Boolean = dependencyState.forall(_.exists)
  }
  private val Initial = CollectingDependencies(extractor.dependsOn.map(k => Missing(k.kind, k.version)))
  case class Ready(dependencyState: Vector[MetadataEntry]) extends HashState {
    override def handle(entry: MetadataEntry): HashState =
      if (entry.kind.kind == extractor.metadataKind.kind)
        if (entry.kind.version == extractor.metadataKind.version) LatestVersion
        else Calculated(entry.kind.version)
      else this
  }
  case class Scheduled(dependencyState: Vector[MetadataEntry]) extends HashState {
    override def handle(entry: MetadataEntry): HashState =
      if (entry.kind.kind == extractor.metadataKind.kind)
        if (entry.kind.version == extractor.metadataKind.version) LatestVersion
        else Calculated(entry.kind.version)
      else this
  }
  case class Calculated(version: Int) extends HashState {
    override def handle(entry: MetadataEntry): HashState =
      if (entry.kind.kind == extractor.metadataKind.kind)
        if (entry.kind.version == extractor.metadataKind.version) LatestVersion
        else Calculated(version max entry.kind.version)
      else this
    // FIXME: handle dependency changes
  }
  private val LatestVersion = Calculated(extractor.metadataKind.version)
  case class PreConditionNotMet(cause: String) extends HashState {
    override def handle(entry: MetadataEntry): HashState = {
      require(
        !(entry.kind.kind == extractor.metadataKind.kind && entry.kind.version == extractor.metadataKind.version),
        s"Unexpected metadata entry found where previously precondition was not met because of [$cause]")
      this
    }
  }

  type Api = MetadataExtractionScheduler

  override val id: String = s"net.virtualvoid.fotofinish.metadata[${extractor.kind}]"
  def version: Int = 3

  def initialPerHashState(hash: Hash): HashState = Initial
  def processEvent(hash: Hash, state: HashState, event: MetadataEnvelope): HashState = state.handle(event.entry)

  def hasWork(hash: Hash, state: HashState): Boolean = state.isInstanceOf[Ready]
  def createWork(hash: Hash, state: HashState, context: ExtractionContext): (HashState, Vector[WorkEntry]) =
    state match {
      case Ready(depValues) =>
        (
          Scheduled(depValues),
          Vector(WorkEntry.opaque(() => extractor.extract(hash, depValues, context).map(Vector(_))(context.executionContext)))
        )
      case _ => (state, Vector.empty)
    }
  def api(handleWithState: PerHashHandleWithStateFunc[HashState])(implicit ec: ExecutionContext): MetadataExtractionScheduler =
    new MetadataExtractionScheduler {
      def workHistogram: Future[Map[String, Int]] =
        handleWithState.accessAll { states =>
          states
            .toVector // FIXME: could we somehow support these queries in a better way? (e.g. accumulating numbers directly)
            .groupBy(_._2.productPrefix)
            .view.mapValues(_.size).toMap
        }
    }

  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[HashState] = {
    import JsonExtra._
    import spray.json.DefaultJsonProtocol._
    import spray.json._

    implicit def missingFormat: JsonFormat[Missing] = jsonFormat2(Missing)
    implicit def existingFormat: JsonFormat[Existing] = jsonFormat1(Existing)
    implicit def depStateFormat: JsonFormat[DependencyState] = new JsonFormat[DependencyState] {
      def read(json: JsValue): DependencyState =
        json.field("type") match {
          case JsString("Missing")  => json.convertTo[Missing]
          case JsString("Existing") => json.convertTo[Existing]
          case x                    => throw DeserializationException(s"Unexpected type '$x' for DependencyState")
        }
      def write(obj: DependencyState): JsValue = obj match {
        case m: Missing  => m.toJson + ("type" -> JsString("Missing"))
        case e: Existing => e.toJson + ("type" -> JsString("Existing"))
      }
    }

    implicit def collectingFormat: JsonFormat[CollectingDependencies] = jsonFormat1(CollectingDependencies.apply)
    implicit def readyFormat: JsonFormat[Ready] = jsonFormat1(Ready.apply)
    implicit def scheduledFormat: JsonFormat[Scheduled] = jsonFormat1(Scheduled.apply)
    implicit def calculatedFormat: JsonFormat[Calculated] = jsonFormat1(Calculated.apply)
    implicit def preconditionNotMetFormat: JsonFormat[PreConditionNotMet] = jsonFormat1(PreConditionNotMet.apply)
    implicit def hashStateFormat: JsonFormat[HashState] = new JsonFormat[HashState] {
      import net.virtualvoid.fotofinish.util.JsonExtra._
      override def read(json: JsValue): HashState =
        json.field("type") match {
          case JsString("CollectingDependencies") => json.convertTo[CollectingDependencies]
          case JsString("Ready")                  => json.convertTo[Ready]
          case JsString("Scheduled")              => json.convertTo[Scheduled]
          case JsString("Calculated")             => json.convertTo[Calculated]
          case JsString("PreConditionNotMet")     => json.convertTo[PreConditionNotMet]
          case x                                  => throw DeserializationException(s"Unexpected type '$x' for HashState")
        }

      override def write(obj: HashState): JsValue = obj match {
        case c: CollectingDependencies => c.toJson + ("type" -> JsString("CollectingDependencies"))
        case r: Ready                  => r.toJson + ("type" -> JsString("Ready"))
        case s: Scheduled              => s.toJson + ("type" -> JsString("Scheduled"))
        case c: Calculated             => c.toJson + ("type" -> JsString("Calculated"))
        case p: PreConditionNotMet     => p.toJson + ("type" -> JsString("PreConditionNotMet"))
      }
    }
    hashStateFormat
  }

  override def initializeStateSnapshot(hash: Hash, state: HashState): HashState =
    state match {
      case Scheduled(depValues) => Ready(depValues)
      case x                    => x
    }
}

trait MetadataApi {
  def metadataFor(id: Id): Future[Metadata]
  def knownObjects(): Future[TreeSet[Id]]
}

object PerObjectMetadataCollector extends SimplePerHashProcess {
  type PerHashState = Metadata
  override type Api = MetadataApi

  def version: Int = 2

  def initialPerHashState(hash: Hash): Metadata = Metadata(Vector.empty)
  def processEvent(hash: Hash, state: Metadata, event: MetadataEnvelope): Metadata =
    state.copy(entries = state.entries :+ event.entry)

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
