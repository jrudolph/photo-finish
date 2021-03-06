package net.virtualvoid.fotofinish.process

import net.virtualvoid.fotofinish.Hash
import net.virtualvoid.fotofinish.metadata._
import net.virtualvoid.fotofinish.util.DeduplicationCache

import scala.concurrent.{ ExecutionContext, Future }

trait SimilarFaces {
  def similarFacesTo(hash: Hash, idx: Int): Future[Vector[(Hash, Int, Float)]]
}

class PerFaceDistanceCollector(threshold: Float) extends PerKeyProcess {
  type Key = FaceId
  type GlobalState = Global
  type PerKeyState = PerFace
  type Api = SimilarFaces

  private val effectiveThreshold = FaceUtils.thresholdFromFloat(threshold)
  private val faceIdCache = DeduplicationCache[FaceId]()

  case class Global(
      vectors: Vector[(FaceUtils.FeatureVector, FaceId)]
  ) {
    def handle(hash: Hash, data: FaceData): (Global, Effect) = {
      def faceId(idx: Int, faceInfo: FaceInfo): FaceId = faceIdCache(FaceId(hash, idx)) //, faceInfo.rectangle)
      def face(idx: Int, faceInfo: FaceInfo): PerFace = PerFace(faceId(idx, faceInfo), FaceUtils.createVector(faceInfo.modelData), Set.empty)
      val allFaces = data.faces.zipWithIndex.map { case (info, idx) => face(idx, info) }

      val newVectors = vectors ++ allFaces.map(f => (f.vector, f.faceId))

      def injectFace(p: PerFace): Effect = {
        val neighbors =
          vectors
            .map(e => e._2 -> FaceUtils.sqdist(e._1, p.vector, effectiveThreshold))
            .filter(_._2 < effectiveThreshold)
            .filter(_._1 != p.faceId)

        val newFace = p.copy(neighbors = neighbors.toSet)
        def forNeighbor(n: (FaceId, Int)): Effect =
          Effect.mapKeyState(n._1)(_.addNeighbor(p.faceId, n._2))

        Effect
          .setKeyState(p.faceId, newFace)
          .and(neighbors.map(forNeighbor))
      }

      val effects = allFaces.map(injectFace)

      copy(vectors = newVectors) -> Effect.and(effects)
    }
  }

  case class FaceId(
      hash: Hash,
      idx:  Int
  //rectangle: Rectangle
  )
  case class PerFace(
      faceId:    FaceId,
      vector:    FaceUtils.FeatureVector,
      neighbors: Set[(FaceId, Int)]
  ) {
    def addNeighbor(f: FaceId, distance: Int): PerFace =
      if (f != faceId) copy(neighbors = neighbors + (f -> distance))
      else this
  }

  def version: Int = 4
  def initialGlobalState: Global = Global(Vector.empty)
  def initialPerKeyState(key: FaceId): PerFace = PerFace(key, Array.empty, Set.empty)
  def processEvent(event: MetadataEnvelope): Effect =
    event.entry.kind match {
      case FaceData => Effect.flatMapGlobalState(_.handle(event.entry.target.hash, event.entry.value.asInstanceOf[FaceData]))
      case _        => Effect.Empty
    }

  def api(handleWithState: AccessStateFunc)(implicit ec: ExecutionContext): SimilarFaces =
    new SimilarFaces {
      def similarFacesTo(hash: Hash, idx: Int): Future[Vector[(Hash, Int, Float)]] =
        handleWithState.access(faceIdCache(FaceId(hash, idx)))(_.neighbors.toVector.map { case (FaceId(hash, idx), dist) => (hash, idx, dist.toFloat / FaceUtils.Factor / FaceUtils.Factor) })
    }
  import spray.json._
  import DefaultJsonProtocol._
  implicit val faceIdFormat: JsonFormat[FaceId] = DeduplicationCache.cachedFormat(jsonFormat2(FaceId), faceIdCache)
  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[PerFace] = jsonFormat3(PerFace)
  def globalStateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[Global] =
    jsonFormat1(Global.apply _)

  override def deserializeKey(keyString: String): FaceId = {
    val Array(hash, idx) = keyString.split("#")
    faceIdCache(FaceId(Hash.fromPrefixedString(hash).get, idx.toInt))
  }
  override def serializeKey(key: FaceId): String = s"${key.hash.toString}#${key.idx}"

}

object FaceUtils {
  // 128 would be the safe value if we'd expect the embedding vector values to range from [-1, 1[. However, it seems that
  // not the whole range is used, so we spread it a bit more for slightly better accuracy (createVector checks that we don't overrun)
  val Factor = 200
  def thresholdFromFloat(thres: Float): Int = {
    val x = (thres * Factor).toInt
    x * x
  }
  def createVector(fs: Array[Float]): FeatureVector =
    fs.map { f =>
      val i = (f * Factor).toInt
      val res = i.toByte
      if (i != res) throw new IllegalStateException(s"$f $i $res")
      res
    }

  type FeatureVector = Array[Byte]
  def sqdist(a1: FeatureVector, a2: FeatureVector, threshold: Int): Int = {
    /* naive but slow because of boxing algorithm
    (a1, a2).zipped
      .map {
        case (a, b) =>
          val diff = a - b
          diff * diff
      }.sum
     */

    var i = 0
    var sqDiff = 0
    while (i < a1.length && sqDiff < threshold) {
      val x1 = a1(i)
      val x2 = a2(i)
      val diff = x1 - x2

      sqDiff += diff * diff
      //println(f"x1: $x1%3d x2: $x2%3d diff: $diff%5d sqDiff:$sqDiff%5d")

      i += 1
    }
    sqDiff
  }
}
