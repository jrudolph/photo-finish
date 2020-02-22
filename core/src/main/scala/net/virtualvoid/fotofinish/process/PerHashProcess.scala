package net.virtualvoid.fotofinish.process

import java.io.File
import java.sql.{ Connection, DriverManager, ResultSet }

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Sink }
import net.virtualvoid.fotofinish.metadata.{ ExtractionContext, MetadataEntry, MetadataEnvelope }
import net.virtualvoid.fotofinish.{ Hash, RepositoryConfig }
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
trait PerHashProcessWithNoGlobalState extends PerHashProcess {
  type GlobalState = AnyRef
  def initialGlobalState: AnyRef = null
  def globalStateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[AnyRef] = new JsonFormat[AnyRef] {
    override def write(obj: AnyRef): JsValue = JsNull
    override def read(json: JsValue): AnyRef = null
  }
}

trait PerHashProcess extends PerKeyProcess {
  type Key = Hash
  type PerHashHandleWithStateFunc[S] = PerKeyHandleWithStateFunc[Hash, S]

  def processHashEvent(hash: Hash, state: PerKeyState, event: MetadataEnvelope): Effect

  override def processEvent(event: MetadataEnvelope): Effect = {
    val hash = event.entry.target.hash
    Effect.flatMapKeyState(hash) { state =>
      state -> processHashEvent(hash, state, event)
    }
  }
  override def serializeKey(key: Hash): String = key.toString
  override def deserializeKey(keyString: String): Hash = Hash.fromPrefixedString(keyString).get
}

trait PerKeyProcess { pkp => // FIXME: rename
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
  def hasWork(key: Key, state: PerKeyState): Boolean
  def createWork(key: Key, state: PerKeyState, context: ExtractionContext): (PerKeyState, Vector[WorkEntry])
  def api(handleWithState: PerKeyHandleWithStateFunc[Key, PerKeyState])(implicit ec: ExecutionContext): Api

  def isTransient(state: PerKeyState): Boolean = false
  /** Allows to prepare state loaded from snapshot */
  def initializeTransientState(key: Key, state: PerKeyState): PerKeyState = state

  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[PerKeyState]
  def globalStateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[GlobalState]

  /*def toProcess: MetadataProcess { type Api = php.Api } =
    new LineBasedJsonSnaphotProcess {
      case class State(global: GlobalState, data: Map[Hash, PerHashState]) {
        def update[T](hash: Hash)(f: PerHashState => PerHashState): State =
          set(hash, f(get(hash)))
        def get(hash: Hash): PerHashState = data.getOrElse(hash, initialPerHashState(hash))
        def set(hash: Hash, value: PerHashState): State = copy(data = data.updated(hash, value))
      }

      type S = State
      type Api = php.Api
      override def id: String = php.id
      def version: Int = php.version

      def initialState: S = State(initialGlobalState, Map.empty)
      def processEvent(state: State, event: MetadataEnvelope): State = {
        val hash = event.entry.target.hash

        def interpretEffect(state: State, e: php.Effect): State = e match {
          case php.FlatMapHashState(hash, f) =>
            val (newState, nextEffect) = f(state.get(hash))
            interpretEffect(state.set(hash, newState), nextEffect)
          case php.FlatMapGlobalState(f) =>
            val (newState, nextEffect) = f(state.global)
            interpretEffect(state.copy(global = newState), nextEffect)
          case php.Multiple(head +: tail) =>
            val newState = interpretEffect(state, head)
            if (tail.isEmpty) newState
            else interpretEffect(newState, Multiple(tail))
          case Effect.Empty => state
        }

        interpretEffect(state, php.processEvent(hash, state.get(hash), event))
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
        state.copy(data = state.data.map { case (k, v) => k -> php.initializeTransientState(k, v) })

      type StateEntryT = (Hash, PerHashState)
      def stateAsEntries(state: State): Iterator[(Hash, PerHashState)] = state.data.iterator
      def entriesAsState(entries: Iterable[(Hash, PerHashState)]): State = ??? // State(entries.toMap)
      def stateEntryFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[(Hash, PerHashState)] = {
        import spray.json.DefaultJsonProtocol._
        implicit val phsFormat = php.stateFormat
        implicitly[JsonFormat[(Hash, PerHashState)]]
      }
    }*/

  def toProcessSqlite(implicit entryFormat: JsonFormat[MetadataEntry]): MetadataProcess { type Api = pkp.Api } =
    new MetadataProcess {
      type S = State
      type Api = pkp.Api
      override def id: String = pkp.id
      def version: Int = pkp.version

      case class State(
          global:     GlobalState,
          cachedData: Map[Key, PerKeyState],
          dirty:      Set[Key],
          newEntries: Set[Key],
          withWork:   Set[Key],
          transient:  Set[Key],
          connection: Option[Connection]) {
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
          val stmt = conn.prepareStatement("select data from per_hash_data where hash = ?")
          stmt.setString(1, serializeKey(key))
          val rs = stmt.executeQuery()

          if (rs.next()) Some(readFromRS(rs))
          else None
        }
        def data: Iterator[(Key, PerKeyState)] = {
          val existing =
            connection.iterator.flatMap { conn =>
              val rs =
                conn.createStatement()
                  .executeQuery("select hash, data from per_hash_data")
              Iterator.unfold(rs) { rs =>
                if (rs.next()) Some {
                  import spray.json._
                  implicit val phsFormat = pkp.stateFormat
                  val key = deserializeKey(rs.getString("hash"))

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
                conn.createStatement()
                  .executeQuery("select hash from per_hash_data")

              Iterator.unfold(rs) { rs =>
                if (rs.next()) Some {
                  (deserializeKey(rs.getString("hash")), rs)
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
              val (newState, work) = pkp.createWork(key, phs, context)
              (
                (s: State) => s.update(key)(_ => newState).removeFromWorkList(key),
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
                case (hash, f) => state =>
                  val (newState, entries) = f(state.get(hash))
                  (state.update(hash)(_ => newState), entries)

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
        val conn = state.connection.getOrElse(openConnectionTo(target))
        val stmt = conn.createStatement

        def pragmas(): Unit = {
          stmt.execute("pragma journal_mode=wal")
          stmt.execute("pragma page_size=65536")
          stmt.execute("pragma synchronous=0")
        }
        def setupTables(): Unit = {
          stmt.execute(s"create table if not exists per_hash_data(hash PRIMARY KEY, data)")
          stmt.execute("create table if not exists meta(process_id, process_version, seq_nr, global_state)")
          stmt.execute("create table if not exists has_work(hash)")
          stmt.execute("create table if not exists transient(hash)")
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

          val insert = conn.prepareStatement(s"insert or replace into per_hash_data(hash, data) values (?, ?)")
          println(s"[$id] Dirty set: ${state.dirty.size}")
          val toFlush =
            state.dirty
              .map { key =>
                serializeKey(key) -> state.cachedData(key).toJson.compactPrint
              }

          toFlush
            .foreach {
              case (key, data) =>
                insert.setString(1, key)
                insert.setString(2, data)
                insert.execute()
            }
        }
        def insertSet(tableName: String, set: Set[Key]): Unit = {
          val insert = conn.prepareStatement(s"insert into $tableName(hash) values(?)")
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
        state.copy(dirty = Set.empty, newEntries = Set.empty, connection = Some(conn)) // TODO: drop part of the cache?
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
            val rs2 = stmt.executeQuery(s"select hash from $tableName")
            Iterator.unfold(rs2) { rs =>
              if (rs.next()) Some {
                (deserializeKey(rs.getString("hash")), rs)
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
              State(globalState, Map.empty, Set.empty, Set.empty, hasWork, transient, Some(conn))
            )
          }
        } else None

      private def openConnectionTo(target: File): Connection = {
        val url = s"jdbc:sqlite:${target.getAbsolutePath}"
        DriverManager.getConnection(url)
      }
    }
}