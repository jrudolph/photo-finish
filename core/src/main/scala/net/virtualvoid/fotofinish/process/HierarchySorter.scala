package net.virtualvoid.fotofinish.process

import akka.http.scaladsl.model.DateTime
import net.virtualvoid.fotofinish.Hash
import net.virtualvoid.fotofinish.metadata._
import spray.json.{ DefaultJsonProtocol, JsonFormat }

import scala.concurrent.{ ExecutionContext, Future }

sealed trait Node[T] {
  def name: T
  def fullPath: Vector[T]
  def entries: Future[Map[T, Vector[Hash]]]
  def children: Future[Map[T, Node[T]]]
}

trait HierarchyAccess[T] {
  def root: Future[Node[T]]
  def byPrefix(prefixSegments: Vector[T]): Future[Option[Node[T]]]
}

trait Hierarchy[T] {
  type M
  def metadataKind: MetadataKind.Aux[M]
  def extract(hash: Hash, entry: M): Vector[T]
  def rootName: T

  def tFormat: JsonFormat[T]
}

object OriginalFileNameHierarchy extends Hierarchy[String] {
  type M = IngestionData
  def metadataKind: MetadataKind.Aux[IngestionData] = IngestionData

  def extract(hash: Hash, entry: IngestionData): Vector[String] = entry.originalFullFilePath.split("/").toVector.drop(1)
  def rootName: String = ""
  def tFormat: JsonFormat[String] = DefaultJsonProtocol.StringJsonFormat
}
object YearMonthHierarchy extends Hierarchy[String] {
  type M = ExifBaseData
  def metadataKind: MetadataKind.Aux[ExifBaseData] = ExifBaseData

  def extract(hash: Hash, entry: ExifBaseData): Vector[String] = {
    val dateTime = entry.dateTaken.getOrElse(DateTime(0L))
    Vector(dateTime.year.toString, dateTime.month.toString, s"${hash.asHexString}.jpg")
  }
  def rootName: String = ""
  def tFormat: JsonFormat[String] = DefaultJsonProtocol.StringJsonFormat
}

class HierarchySorter[T](hierarchy: Hierarchy[T]) extends SingleEntryState {
  override type S = State
  override type Api = HierarchyAccess[T]

  override def id: String = super.id + "." + hierarchy.getClass.getName

  case class NodeImpl(fullPath: Vector[T], children: Map[T, NodeImpl], entries: Map[T, Vector[Hash]]) {
    def enter(hash: Hash, remainingPathSegments: Vector[T]): NodeImpl =
      remainingPathSegments match {
        case head +: tail =>
          if (tail.isEmpty) {
            val existingEntries = entries.getOrElse(head, Vector.empty)
            copy(entries = entries + (head -> (existingEntries :+ hash)))
          } else {
            val node = children.getOrElse(head, NodeImpl(fullPath :+ head, Map.empty, Map.empty))
            copy(children = children.updated(head, node.enter(hash, tail)))
          }
      }

    def get(prefix: Vector[T]): Option[NodeImpl] =
      prefix match {
        case head +: tail => children.get(head).flatMap(_.get(tail))
      }
  }

  case class State(root: NodeImpl) {
    def handle(hash: Hash, data: hierarchy.M): State =
      copy(root.enter(hash, hierarchy.extract(hash, data)))
  }

  override def version: Int = 1
  override def initialState: State = State(NodeImpl(Vector.empty, Map.empty, Map.empty))
  override def processEvent(state: State, event: MetadataEnvelope): State =
    if (event.entry.kind == hierarchy.metadataKind) state.handle(event.entry.target.hash, event.entry.value.asInstanceOf[hierarchy.M])
    else state

  override def createWork(state: State, context: ExtractionContext): (State, Vector[WorkEntry]) = (state, Vector.empty)
  override def api(handleWithState: HandleWithStateFunc[State])(implicit ec: ExecutionContext): HierarchyAccess[T] =
    new HierarchyAccess[T] {
      def root: Future[Node[T]] = byPrefix(Vector.empty).map(_.get)

      def byPrefix(prefixSegments: Vector[T]): Future[Option[Node[T]]] =
        handleWithState.access(_.root.get(prefixSegments).map(n(handleWithState)))

      private def n(handleWithState: HandleWithStateFunc[State])(node: NodeImpl): Node[T] = new Node[T] {
        override def name: T = node.fullPath.lastOption.getOrElse(hierarchy.rootName)
        override def fullPath: Vector[T] = node.fullPath
        override def entries: Future[Map[T, Vector[Hash]]] =
          handleWithState.access(_.root.get(fullPath).fold(Map.empty[T, Vector[Hash]])(_.entries))
        override def children: Future[Map[T, Node[T]]] =
          handleWithState.access(_.root.get(fullPath).fold(Map.empty[T, Node[T]])(_.children.view.mapValues(v => n(handleWithState)(v)).toMap))
      }
    }

  import spray.json._
  import DefaultJsonProtocol._
  private implicit lazy val nodeImplFormat: JsonFormat[NodeImpl] = {
    implicit val tFormat = hierarchy.tFormat
    lazyFormat(jsonFormat3(NodeImpl.apply))
  }
  private val theStateFormat: JsonFormat[State] = jsonFormat1(State.apply)
  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[State] = theStateFormat
}