package net.virtualvoid.fotofinish.process

import java.io.File
import java.sql.{ Connection, DriverManager, ResultSet }

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Sink }
import net.virtualvoid.fotofinish.{ Hash, RepositoryConfig }
import net.virtualvoid.fotofinish.metadata.{ ExtractionContext, MetadataEntry, MetadataEnvelope }
import spray.json.JsonFormat

import scala.concurrent.{ ExecutionContext, Future }

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

trait PerHashProcess { php =>
  type PerHashState
  type Api

  def id: String = getClass.getName
  def version: Int

  def initialPerHashState(hash: Hash): PerHashState
  def processEvent(hash: Hash, state: PerHashState, event: MetadataEnvelope): PerHashState
  def hasWork(hash: Hash, state: PerHashState): Boolean
  def createWork(hash: Hash, state: PerHashState, context: ExtractionContext): (PerHashState, Vector[WorkEntry])
  def api(handleWithState: PerHashHandleWithStateFunc[PerHashState])(implicit ec: ExecutionContext): Api

  def isTransient(state: PerHashState): Boolean = false
  /** Allows to prepare state loaded from snapshot */
  def initializeTransientState(hash: Hash, state: PerHashState): PerHashState = state

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
        State(state.data.map { case (k, v) => k -> php.initializeTransientState(k, v) })

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
          transient:  Set[Hash],
          connection: Option[Connection]) {
        def update(hash: Hash)(f: PerHashState => PerHashState): State = {
          val existing = getIfExists(hash)
          val newState = f(existing.getOrElse(initialPerHashState(hash)))
          val newNew = if (existing.isDefined) newEntries else newEntries + hash
          val hasW = hasWork(hash, newState)
          val newWithWork =
            if (hasW) withWork + hash
            else withWork - hash
          val isT = isTransient(newState)
          val newTransient =
            if (isT) transient + hash
            else transient - hash

          copy(
            cachedData = cachedData + (hash -> newState),
            dirty = dirty + hash,
            newEntries = newNew,
            withWork = newWithWork,
            transient = newTransient
          )
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

        def updateTransient: State =
          transient.foldLeft(this) { (state, hash) =>
            state.update(hash)(phs => php.initializeTransientState(hash, phs))
          }
      }

      def initialState: S = State(Map.empty, Set.empty, Set.empty, Set.empty, Set.empty, None)
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

      override def initializeStateSnapshot(state: State): State = state.updateTransient

      def saveSnapshot(target: File, config: RepositoryConfig, snapshot: Snapshot[State]): State = {
        val state = snapshot.state
        val conn = state.connection.getOrElse(openConnectionTo(target))
        val stmt = conn.createStatement

        def setupTables(): Unit = {
          stmt.execute(s"create table if not exists per_hash_data(hash PRIMARY KEY, data)")
          stmt.execute("create table if not exists meta(process_id, process_version, seq_nr)")
          stmt.execute("create table if not exists has_work(hash)")
          stmt.execute("create table if not exists transient(hash)")
          stmt.executeUpdate("delete from meta")
          // FIXME: do we have better ideas to manage those then to recreate them from scratch? Probably not a problem currently, but has_work could
          // become quite big
          stmt.executeUpdate("delete from has_work")
          stmt.executeUpdate("delete from transient")
        }

        def setMeta(): Unit = {
          val prepared = conn.prepareStatement("insert into meta(process_id, process_version, seq_nr) values(?, ?, ?)")
          prepared.setString(1, snapshot.processId)
          prepared.setInt(2, snapshot.processVersion)
          prepared.setLong(3, snapshot.currentSeqNr)
          prepared.executeUpdate() // TODO: check for result
        }
        def insertDirty(): Unit = {
          import spray.json._
          implicit val phsFormat = php.stateFormat

          val insert = conn.prepareStatement(s"insert or replace into per_hash_data(hash, data) values (?, ?)")
          println(s"[$id] Dirty set: ${state.dirty.size}")
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
        }
        def insertSet(tableName: String, set: Set[Hash]): Unit = {
          val insert = conn.prepareStatement(s"insert into $tableName(hash) values(?)")
          set.foreach { h =>
            insert.setString(1, h.toString)
            insert.execute()
          }
        }

        stmt.execute("begin transaction")
        setupTables()
        setMeta()
        insertDirty()
        insertSet("has_work", state.withWork)
        insertSet("transient", state.transient)
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

          def loadSet(tableName: String): Set[Hash] = {
            val rs2 = stmt.executeQuery(s"select hash from $tableName")
            Iterator.unfold(rs2) { rs =>
              if (rs.next()) Some {
                (Hash.fromPrefixedString(rs.getString("hash")).get, rs)
              }
              else None
            }.toSet
          }
          val hasWork = loadSet("has_work")
          val transient = loadSet("transient")

          Some {
            Snapshot(
              processId,
              processVersion,
              seqNr,
              State(Map.empty, Set.empty, Set.empty, hasWork, transient, Some(conn))
            )
          }
        } else None

      private def openConnectionTo(target: File): Connection = {
        val url = s"jdbc:sqlite:${target.getAbsolutePath}"
        DriverManager.getConnection(url)
      }
    }
}