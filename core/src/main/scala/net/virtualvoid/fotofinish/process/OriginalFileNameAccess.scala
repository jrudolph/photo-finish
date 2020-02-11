package net.virtualvoid.fotofinish.process

import net.virtualvoid.fotofinish.Hash
import net.virtualvoid.fotofinish.metadata.{ ExtractionContext, IngestionData, MetadataEntry, MetadataEnvelope }

import scala.concurrent.{ ExecutionContext, Future }

sealed trait Node {
  def name: String
  def fullPath: Vector[String]
  def entries: Future[Map[String, Vector[Hash]]]
  def children: Future[Map[String, Node]]
}

trait ByOriginalFileName {
  def root: Future[Node]
  def byPrefix(prefixSegments: Vector[String]): Future[Option[Node]]
}

object OriginalFileNameAccess extends SingleEntryState {
  override type S = State
  override type Api = ByOriginalFileName

  case class NodeImpl(fullPath: Vector[String], children: Map[String, NodeImpl], entries: Map[String, Vector[Hash]]) {
    def enter(hash: Hash, remainingPathSegments: Vector[String]): NodeImpl =
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

    def get(prefix: Vector[String]): Option[NodeImpl] =
      prefix match {
        case head +: tail => children.get(head).flatMap(_.get(tail))
      }
  }

  case class State(root: NodeImpl) {
    def handle(hash: Hash, data: IngestionData): State =
      copy(root.enter(hash, data.originalFullFilePath.split("/").toVector.drop(1)))
  }

  override def version: Int = 1
  override def initialState: State = State(NodeImpl(Vector.empty, Map.empty, Map.empty))
  override def processEvent(state: State, event: MetadataEnvelope): State = event.entry.kind match {
    case IngestionData => state.handle(event.entry.target.hash, event.entry.value.asInstanceOf[IngestionData])
    case x             => state
  }
  override def createWork(state: State, context: ExtractionContext): (State, Vector[WorkEntry]) = (state, Vector.empty)
  override def api(handleWithState: HandleWithStateFunc[State])(implicit ec: ExecutionContext): ByOriginalFileName =
    new ByOriginalFileName {
      def root: Future[Node] = ???
      def byPrefix(prefixSegments: Vector[String]): Future[Option[Node]] =
        handleWithState.access(_.root.get(prefixSegments).map(n(handleWithState)))

      private def n(handleWithState: HandleWithStateFunc[State])(node: NodeImpl): Node = new Node {
        override def name: String = node.fullPath.lastOption.getOrElse("")
        override def fullPath: Vector[String] = node.fullPath
        override def entries: Future[Map[String, Vector[Hash]]] =
          handleWithState.access(_.root.get(fullPath).fold(Map.empty[String, Vector[Hash]])(_.entries))
        override def children: Future[Map[String, Node]] =
          handleWithState.access(_.root.get(fullPath).fold(Map.empty[String, Node])(_.children.view.mapValues(v => n(handleWithState)(v)).toMap))
      }
    }

  import spray.json._
  import DefaultJsonProtocol._
  private implicit lazy val nodeImplFormat: JsonFormat[NodeImpl] = lazyFormat(jsonFormat3(NodeImpl.apply))
  private val theStateFormat: JsonFormat[State] = jsonFormat1(State.apply)
  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[State] = theStateFormat
}
