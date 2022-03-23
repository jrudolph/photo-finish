package net.virtualvoid.fotofinish.process

import net.virtualvoid.fotofinish.Hash
import net.virtualvoid.fotofinish.metadata.{ MetadataEntry, MetadataEnvelope, PHashData }
import spray.json.JsonFormat

import scala.concurrent.{ ExecutionContext, Future }

trait SimilarImages {
  def similarImagesTo(hash: Hash): Future[Vector[(Hash, Int)]]
}

class PHashDistanceCollector(threshold: Int) extends PerKeyProcess {
  type Key = Hash
  type GlobalState = Global
  type PerKeyState = KeyState
  type Api = SimilarImages

  def version: Int = 1

  case class Global(images: Vector[(Hash, Long)]) {
    def handle(hash: Hash, phash: Long): (Global, Effect) = {
      val newImages = images :+ (hash, phash)

      def injectImage(hash: Hash, phash: Long): Effect = {
        val neighbors =
          images
            .map(e => e._1 -> distance(e._2, phash))
            .filter(_._2 < threshold)
            .filter(_._1 != hash)

        if (neighbors.nonEmpty)
          println(s"${Console.BLUE}[$hash] found ${neighbors.size} neighbors: [${neighbors.mkString(", ")}]${Console.RESET}")

        val newFace = KeyState(phash, neighbors)
        def forNeighbor(n: (Hash, Int)): Effect =
          Effect.mapKeyState(n._1)(_.addNeighbor(hash, n._2))

        Effect
          .setKeyState(hash, newFace)
          .and(neighbors.map(forNeighbor))
      }

      copy(images = newImages) -> injectImage(hash, phash)
    }
  }
  case class KeyState(pHash: Long, neighbors: Vector[(Hash, Int)]) {
    def addNeighbor(hash: Hash, distance: Int): KeyState = copy(neighbors = neighbors :+ hash -> distance)
  }

  def initialGlobalState: Global = Global(Vector.empty)
  def initialPerKeyState(key: Hash): KeyState = KeyState(0L, Vector.empty)

  def processEvent(event: MetadataEnvelope): Effect =
    event.entry.kind match {
      case PHashData => Effect.flatMapGlobalState(_.handle(event.entry.target.hash, event.entry.value.asInstanceOf[PHashData].entries.head._2))
      case _         => Effect.Empty
    }
  def api(handleWithState: AccessStateFunc)(implicit ec: ExecutionContext): SimilarImages =
    new SimilarImages {
      def similarImagesTo(hash: Hash): Future[Vector[(Hash, Int)]] =
        handleWithState.access(hash) { state =>
          state.neighbors.map {
            case (hash, dist) => hash -> dist
          }
        }
    }

  def distance(h1: Long, h2: Long): Int = java.lang.Long.bitCount(h1 ^ h2)

  def serializeKey(key: Hash): String = key.toString
  def deserializeKey(keyString: String): Hash = Hash.fromPrefixedString(keyString).get
  import spray.json.DefaultJsonProtocol._
  def globalStateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[Global] = jsonFormat1(Global)
  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[KeyState] = jsonFormat2(KeyState)
}
