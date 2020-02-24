package net.virtualvoid.fotofinish.process

import java.io.File
import java.sql.{ Connection, DriverManager, PreparedStatement, ResultSet }

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Sink }
import net.virtualvoid.fotofinish.metadata.{ DeletedMetadata, ExtractionContext, Id, MetadataEntry, MetadataEnvelope }
import net.virtualvoid.fotofinish.RepositoryConfig
import spray.json.{ JsNull, JsValue, JsonFormat }

import scala.concurrent.{ ExecutionContext, Future }

trait PerKeyHandleWithStateFunc[K, S] {
  def apply[T](key: K)(f: S => (S, Vector[WorkEntry], T)): Future[T]
  def access[T](key: K)(f: S => T): Future[T] =
    apply(key) { state =>
      (state, Vector.empty, f(state))
    }

  def accessAll[T](f: Iterator[(K, S)] => T): Future[T]
  def accessAllKeys[T](f: Iterator[K] => T): Future[T]

  def handleStream: Sink[(K, S => (S, Vector[WorkEntry])), Any]
}
trait PerIdProcessWithNoGlobalState extends PerIdProcess {
  type GlobalState = AnyRef
  def initialGlobalState: AnyRef = null
  def globalStateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[AnyRef] = new JsonFormat[AnyRef] {
    override def write(obj: AnyRef): JsValue = JsNull
    override def read(json: JsValue): AnyRef = null
  }
}

trait PerIdProcess extends PerKeyProcess {
  type Key = Id
  type PerIdHandleWithStateFunc[S] = PerKeyHandleWithStateFunc[Id, S]

  def processIdEvent(id: Id, event: MetadataEnvelope): Effect

  override def processEvent(event: MetadataEnvelope): Effect =
    if (event.entry.kind == DeletedMetadata) Effect.Empty
    else processIdEvent(event.entry.target, event)

  override def serializeKey(key: Id): String = key.toString
  override def deserializeKey(keyString: String): Id = Id.fromString(keyString)
}

trait PerKeyProcess { pkp =>
  type Key
  type PerKeyState
  type Api
  type GlobalState

  sealed trait Effect {
    def and(nextEffect: Effect): Effect = Multiple(this, nextEffect)
    def and(effects: Seq[Effect]): Effect = Multiple(Vector(this) ++ effects)
    def accessKeyState(key: Key)(f: PerKeyState => Effect): Effect =
      flatMapKeyState(key)(s => (s, f(s)))
    def setKeyState(key: Key, newState: PerKeyState): Effect =
      mapKeyState(key)(_ => newState)
    def mapKeyState(key: Key)(f: PerKeyState => PerKeyState): Effect =
      flatMapKeyState(key)(s => f(s) -> Effect.Empty)
    def flatMapKeyState(key: Key)(f: PerKeyState => (PerKeyState, Effect)): Effect =
      and(FlatMapKeyState(key, f))

    def flatMapGlobalState(f: GlobalState => (GlobalState, Effect)): Effect =
      and(FlatMapGlobalState(f))
    def mapGlobalState(f: GlobalState => GlobalState): Effect =
      flatMapGlobalState(s => f(s) -> Effect.Empty)
  }
  // the empty effect
  case object Effect extends Effect {
    val Empty: Effect = this
  }
  case class FlatMapGlobalState(f: GlobalState => (GlobalState, Effect)) extends Effect
  case class FlatMapKeyState(key: Key, f: PerKeyState => (PerKeyState, Effect)) extends Effect
  case class Multiple(effects: Vector[Effect]) extends Effect
  object Multiple {
    def apply(first: Effect, next: Effect): Effect = (first, next) match {
      case (Multiple(es1), Multiple(es2)) => Multiple(es1 ++ es2)
      case (Multiple(es1), e2)            => Multiple(es1 :+ e2)
      case (e1, Multiple(es2))            => Multiple(e1 +: es2)
      case (e1, e2)                       => Multiple(Vector(e1, e2))
    }
  }

  def id: String = getClass.getName
  def version: Int

  def deserializeKey(keyString: String): Key
  def serializeKey(key: Key): String

  def initialGlobalState: GlobalState
  def initialPerKeyState(key: Key): PerKeyState
  def processEvent(event: MetadataEnvelope): Effect
  def hasWork(key: Key, state: PerKeyState): Boolean = false
  def createWork(key: Key, state: PerKeyState, context: ExtractionContext): (Effect, Vector[WorkEntry]) = (Effect.Empty, Vector.empty)
  def api(handleWithState: PerKeyHandleWithStateFunc[Key, PerKeyState])(implicit ec: ExecutionContext): Api

  def isTransient(state: PerKeyState): Boolean = false
  /** Allows to prepare state loaded from snapshot */
  def initializeTransientState(key: Key, state: PerKeyState): PerKeyState = state

  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[PerKeyState]
  def globalStateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[GlobalState]

  def toProcessSqlite(implicit entryFormat: JsonFormat[MetadataEntry]): MetadataProcess { type Api = pkp.Api } =
    new MetadataProcess {
      type S = State
      type Api = pkp.Api
      override def id: String = pkp.id
      def version: Int = pkp.version

      class ConnectionInfo(val connection: Connection) {
        lazy val loadDataStatement: PreparedStatement = connection.prepareStatement("select data from key_data where key = ?")
      }

      case class State(
          global:     GlobalState,
          cachedData: Map[Key, PerKeyState],
          dirty:      Set[Key],
          newEntries: Set[Key],
          withWork:   Set[Key],
          transient:  Set[Key],
          connection: Option[ConnectionInfo]) {
        def update(key: Key)(f: PerKeyState => PerKeyState): State = {
          val existing = getIfExists(key)
          val newState = f(existing.getOrElse(initialPerKeyState(key)))
          set(key, newState)
        }
        def set(key: Key, newState: PerKeyState): State = {
          val existing = getIfExists(key)
          val newNew = if (existing.isDefined) newEntries else newEntries + key
          val hasW = hasWork(key, newState)
          val newWithWork =
            if (hasW) withWork + key
            else withWork - key
          val isT = isTransient(newState)
          val newTransient =
            if (isT) transient + key
            else transient - key

          copy(
            cachedData = cachedData + (key -> newState),
            dirty = dirty + key,
            newEntries = newNew,
            withWork = newWithWork,
            transient = newTransient
          )
        }
        def removeFromWorkList(key: Key): State = copy(withWork = withWork - key)
        def getIfExists(key: Key): Option[PerKeyState] =
          cachedData.get(key).orElse(retrieve(key))
        def get(key: Key): PerKeyState =
          getIfExists(key).getOrElse(initialPerKeyState(key))
        def retrieve(key: Key): Option[PerKeyState] = connection.flatMap { conn =>
          val stmt = conn.loadDataStatement
          stmt.setString(1, serializeKey(key))
          val rs = stmt.executeQuery()

          if (rs.next()) Some(readFromRS(rs))
          else None
        }
        def data: Iterator[(Key, PerKeyState)] = {
          val existing =
            connection.iterator.flatMap { conn =>
              val rs =
                conn.connection.createStatement()
                  .executeQuery("select key, data from key_data")
              Iterator.unfold(rs) { rs =>
                if (rs.next()) Some {
                  import spray.json._
                  implicit val phsFormat = pkp.stateFormat
                  val key = deserializeKey(rs.getString("key"))

                  def load(): PerKeyState = rs.getString("data").parseJson.convertTo[pkp.PerKeyState]

                  ((key, cachedData.getOrElse(key, load())), rs)
                }
                else None
              }
            }
          val newE = newEntries.iterator.map(n => n -> cachedData(n))
          existing ++ newE
        }
        def keys: Iterator[Key] = {
          val existing =
            connection.iterator.flatMap { conn =>
              val rs =
                conn.connection.createStatement()
                  .executeQuery("select key from key_data")

              Iterator.unfold(rs) { rs =>
                if (rs.next()) Some {
                  (deserializeKey(rs.getString("key")), rs)
                }
                else None
              }
            }
          existing ++ newEntries.iterator
        }

        private def readFromRS(rs: ResultSet): PerKeyState = {
          import spray.json._
          implicit val phsFormat = pkp.stateFormat
          rs.getString("data").parseJson.convertTo[pkp.PerKeyState]
        }

        def updateTransient: State =
          transient.foldLeft(this) { (state, key) =>
            state.update(key)(phs => pkp.initializeTransientState(key, phs))
          }
      }

      def initialState: S = State(pkp.initialGlobalState, Map.empty, Set.empty, Set.empty, Set.empty, Set.empty, None)
      def processEvent(state: State, event: MetadataEnvelope): State = {
        def interpretEffect(state: State, e: pkp.Effect): State = e match {
          case pkp.FlatMapKeyState(key, f) =>
            val (newState, nextEffect) = f(state.get(key))
            interpretEffect(state.set(key, newState), nextEffect)
          case pkp.FlatMapGlobalState(f) =>
            val (newState, nextEffect) = f(state.global)
            interpretEffect(state.copy(global = newState), nextEffect)
          case pkp.Multiple(head +: tail) =>
            val newState = interpretEffect(state, head)
            if (tail.isEmpty) newState
            else interpretEffect(newState, Multiple(tail))
          case Effect.Empty => state
        }

        interpretEffect(state, pkp.processEvent(event))
      }

      def createWork(state: S, context: ExtractionContext): (S, Vector[WorkEntry]) =
        state.withWork
          .take(10)
          .map(h => h -> state.get(h))
          .map {
            case (key, phs) =>
              val (effect, work) = pkp.createWork(key, phs, context)
              (
                (s: State) => interpretEffect(s, effect).removeFromWorkList(key),
                work
              )
          }
          .foldLeft((state, Vector.empty[WorkEntry])) { (cur, next) =>
            val (s, wes) = cur
            val (f, newWes) = next
            (f(s), wes ++ newWes)
          }

      def api(handleWithState: HandleWithStateFunc[S])(implicit ec: ExecutionContext): Api =
        pkp.api(new PerKeyHandleWithStateFunc[Key, PerKeyState] {
          override def apply[T](key: Key)(f: PerKeyState => (PerKeyState, Vector[WorkEntry], T)): Future[T] =
            handleWithState.apply[T] { state =>
              val (newState, entries, t) = f(state.get(key))
              (state.update(key)(_ => newState), entries, t)
            }

          override def handleStream: Sink[(Key, PerKeyState => (PerKeyState, Vector[WorkEntry])), Any] =
            Flow[(Key, PerKeyState => (PerKeyState, Vector[WorkEntry]))]
              .map[State => (State, Vector[WorkEntry])] {
                case (key, f) => state =>
                  val (newState, entries) = f(state.get(key))
                  (state.update(key)(_ => newState), entries)

              }
              .to(handleWithState.handleStream)

          override def accessAll[T](f: Iterator[(Key, PerKeyState)] => T): Future[T] =
            handleWithState.access(state => f(state.data))

          override def accessAllKeys[T](f: Iterator[Key] => T): Future[T] =
            handleWithState.access(state => f(state.keys))
        })

      override def initializeStateSnapshot(state: State): State = state.updateTransient

      def saveSnapshot(target: File, config: RepositoryConfig, snapshot: Snapshot[State]): State = {
        val state = snapshot.state
        val connectionInfo = state.connection.getOrElse(new ConnectionInfo(openConnectionTo(target)))
        val conn = connectionInfo.connection
        val stmt = conn.createStatement

        def pragmas(): Unit = {
          //stmt.execute("pragma journal_mode=wal")
          stmt.execute("pragma page_size=65536")
          stmt.execute("pragma synchronous=0")
        }
        def setupTables(): Unit = {
          stmt.execute(s"create table if not exists key_data(key PRIMARY KEY, data)")
          stmt.execute("create table if not exists meta(process_id, process_version, seq_nr, global_state)")
          stmt.execute("create table if not exists has_work(key)")
          stmt.execute("create table if not exists transient(key)")
          stmt.executeUpdate("delete from meta")
          // FIXME: do we have better ideas to manage those then to recreate them from scratch? Probably not a problem currently, but has_work could
          // become quite big
          stmt.executeUpdate("delete from has_work")
          stmt.executeUpdate("delete from transient")
        }

        def setMeta(): Unit = {
          val prepared = conn.prepareStatement("insert into meta(process_id, process_version, seq_nr, global_state) values(?, ?, ?, ?)")
          prepared.setString(1, snapshot.processId)
          prepared.setInt(2, snapshot.processVersion)
          prepared.setLong(3, snapshot.currentSeqNr)
          import spray.json._
          prepared.setString(4, snapshot.state.global.toJson(globalStateFormat(config.entryFormat)).compactPrint)
          prepared.executeUpdate() // TODO: check for result
        }
        def insertDirty(): Unit = {
          import spray.json._
          implicit val phsFormat = pkp.stateFormat

          val insert = conn.prepareStatement(s"insert or replace into key_data(key, data) values (?, ?)")
          println(s"[$id] Dirty set: ${state.dirty.size}")
          state.dirty
            .iterator
            .foreach { key =>
              insert.setString(1, serializeKey(key))
              insert.setString(2, state.cachedData(key).toJson.compactPrint)
              insert.execute()
            }
        }
        def insertSet(tableName: String, set: Set[Key]): Unit = {
          val insert = conn.prepareStatement(s"insert into $tableName(key) values(?)")
          set.foreach { h =>
            insert.setString(1, serializeKey(h))
            insert.execute()
          }
        }

        pragmas()
        stmt.execute("begin transaction")
        setupTables()
        setMeta()
        insertDirty()
        insertSet("has_work", state.withWork)
        insertSet("transient", state.transient)
        stmt.execute("commit transaction")
        state.copy(dirty = Set.empty, newEntries = Set.empty, connection = Some(connectionInfo)) // TODO: drop part of the cache?
      }
      def loadSnapshot(target: File, config: RepositoryConfig)(implicit system: ActorSystem): Option[Snapshot[State]] =
        if (target.exists()) {
          val conn = openConnectionTo(target)
          val stmt = conn.createStatement
          val rs = stmt.executeQuery("select process_id, process_version, seq_nr, global_state from meta")
          val processId = rs.getString("process_id")
          val processVersion = rs.getInt("process_version")
          val seqNr = rs.getLong("seq_nr")
          val globalState = {
            import spray.json._
            rs.getString("global_state").parseJson.convertTo[GlobalState](globalStateFormat(config.entryFormat))
          }

          def loadSet(tableName: String): Set[Key] = {
            val rs2 = stmt.executeQuery(s"select key from $tableName")
            Iterator.unfold(rs2) { rs =>
              if (rs.next()) Some {
                (deserializeKey(rs.getString("key")), rs)
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
              State(globalState, Map.empty, Set.empty, Set.empty, hasWork, transient, Some(new ConnectionInfo(conn)))
            )
          }
        } else None

      private def openConnectionTo(target: File): Connection = {
        val url = s"jdbc:sqlite:${target.getAbsolutePath}"
        DriverManager.getConnection(url)
      }
    }
}