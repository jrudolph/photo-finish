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
      def face(idx: Int, faceInfo: FaceInfo): PerFace = PerFace(faceId(idx, faceInfo), FaceUtils.createVector(faceInfo.modelData) /*, Vector.empty*/ )
      val allFaces = data.faces.zipWithIndex.map { case (info, idx) => face(idx, info) }

      val newVectors = vectors ++ allFaces.map(f => (f.vector, f.faceId))
      val insertFaces = allFaces.map(p => Effect.setKeyState(p.faceId, p))
      copy(vectors = newVectors) -> Effect.and(insertFaces)
    }
  }

  case class FaceId(
      hash: Hash,
      idx:  Int
  )
  case class Neighbor(
      hash: Hash,
      idx:  Int,
      dist: Int
  ) {
    def faceId: FaceId = FaceId(hash, idx)
    def hasTarget(f: FaceId): Boolean = f.idx == idx && f.hash == hash
  }
  case class PerFace(
      faceId: FaceId,
      vector: FaceUtils.FeatureVector
  )

  def version: Int = 6
  def initialGlobalState: Global = Global(Vector.empty)
  def initialPerKeyState(key: FaceId): PerFace = PerFace(key, Array.empty)
  def processEvent(event: MetadataEnvelope): Effect =
    event.entry.kind match {
      case FaceData => Effect.flatMapGlobalState(_.handle(event.entry.target.hash, event.entry.value.asInstanceOf[FaceData]))
      case _        => Effect.Empty
    }

  def api(handleWithState: AccessStateFunc)(implicit ec: ExecutionContext): SimilarFaces =
    new SimilarFaces {
      def similarFacesTo(hash: Hash, idx: Int): Future[Vector[(Hash, Int, Float)]] =
        handleWithState.accessGlobal { g =>
          handleWithState.access(faceIdCache(FaceId(hash, idx))) { thisFace =>
            g.vectors
              .map(e => Neighbor(e._2.hash, e._2.idx, FaceUtils.sqdist(e._1, thisFace.vector, effectiveThreshold)))
              .filter(_.dist < effectiveThreshold)
              .map { case Neighbor(hash, idx, dist) => (hash, idx, dist.toFloat / FaceUtils.Factor / FaceUtils.Factor) }
          }
        }.flatten
    }
  import spray.json._
  import DefaultJsonProtocol._
  implicit val faceIdFormat: JsonFormat[FaceId] = DeduplicationCache.cachedFormat(jsonFormat2(FaceId), faceIdCache)
  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[PerFace] = {
    implicit val neighborFormat = jsonFormat3(Neighbor)
    jsonFormat2(PerFace)
  }
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
