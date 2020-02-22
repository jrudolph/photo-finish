package net.virtualvoid.fotofinish.process

import net.virtualvoid.fotofinish.Hash
import net.virtualvoid.fotofinish.metadata._
import spray.json.JsonFormat

import scala.concurrent.{ ExecutionContext, Future }

trait SimilarFaces {
  def similarFacesTo(hash: Hash, idx: Int): Future[Vector[(Hash, Int, Float)]]
}

class FaceDistanceCollector(threshold: Float) extends LineBasedJsonSnaphotProcess {
  private val effectiveThreshold = FaceUtils.thresholdFromFloat(threshold)

  case class FaceId(
      hash: Hash,
      idx:  Int
  //rectangle: Rectangle
  )
  case class PerFace(
      faceId:    FaceId,
      vector:    FaceUtils.FeatureVector,
      neighbors: Vector[(FaceId, Int)]
  ) {
    def addNeighbor(f: FaceId, distance: Int): PerFace = copy(neighbors = neighbors :+ (f -> distance))
  }
  case class State(entries: Map[FaceId, PerFace]) {
    def handle(hash: Hash, data: FaceData): State = {
      def faceId(idx: Int, faceInfo: FaceInfo): FaceId = FaceId(hash, idx) //, faceInfo.rectangle)
      def face(idx: Int, faceInfo: FaceInfo): PerFace = PerFace(faceId(idx, faceInfo), FaceUtils.createVector(faceInfo.modelData), Vector.empty)

      val allFaces = data.faces.zipWithIndex.map { case (info, idx) => face(idx, info) }

      allFaces.foldLeft(this)(_.inject(_))
    }

    def inject(perFace: PerFace): State = {
      val neighbors =
        entries
          .values
          .map(e => e -> FaceUtils.sqdist(e.vector, perFace.vector, effectiveThreshold))
          .filter(_._2 < effectiveThreshold)

      neighbors.foldLeft(addFace(perFace))((state, f) => state.addNeighbor(f._1.faceId, perFace.faceId, f._2))
    }
    def addFace(perFace: PerFace): State = copy(entries = entries + (perFace.faceId -> perFace))
    def update(face: FaceId, f: PerFace => PerFace): State = copy(entries = entries + (face -> f(entries(face))))
    def addNeighbor(f1: FaceId, f2: FaceId, distance: Int): State =
      update(f1, _.addNeighbor(f2, distance))
        .update(f2, _.addNeighbor(f1, distance))

    def similarFacesTo(hash: Hash, idx: Int): Vector[(Hash, Int, Float)] =
      entries.get(FaceId(hash, idx)).fold(Vector.empty[(Hash, Int, Float)])(_.neighbors.map { case (FaceId(hash, idx), dist) => (hash, idx, dist.toFloat / FaceUtils.Factor / FaceUtils.Factor) })
  }

  type S = State
  type Api = SimilarFaces

  def version: Int = 1
  def initialState: State = State(Map.empty)
  def processEvent(state: State, event: MetadataEnvelope): State = event.entry.kind match {
    case FaceData => state.handle(event.entry.target.hash, event.entry.value.asInstanceOf[FaceData])
    case _        => state
  }
  def createWork(state: State, context: ExtractionContext): (State, Vector[WorkEntry]) = (state, Vector.empty)
  def api(handleWithState: HandleWithStateFunc[State])(implicit ec: ExecutionContext): SimilarFaces =
    new SimilarFaces {
      def similarFacesTo(hash: Hash, idx: Int): Future[Vector[(Hash, Int, Float)]] =
        handleWithState.access(_.similarFacesTo(hash, idx))
    }

  type StateEntryT = (FaceId, PerFace)
  def stateAsEntries(state: State): Iterator[StateEntryT] = state.entries.iterator
  def entriesAsState(entries: Iterable[StateEntryT]): State = State(Map(entries.toVector: _*))
  def stateEntryFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[StateEntryT] = {
    import spray.json._
    import DefaultJsonProtocol._
    implicit val faceIdFormat: JsonFormat[FaceId] = jsonFormat2(FaceId)
    implicit val perFaceFormat: JsonFormat[PerFace] = jsonFormat3(PerFace)
    implicitly[JsonFormat[StateEntryT]]
  }
}

class PerHashFaceDistanceCollector(threshold: Float) extends PerKeyProcess {
  type Key = FaceId
  type GlobalState = Global
  type PerKeyState = PerFace
  type Api = SimilarFaces

  private val effectiveThreshold = FaceUtils.thresholdFromFloat(threshold)

  case class Global(
      vectors: Vector[(FaceUtils.FeatureVector, FaceId)]
  ) {
    def handle(hash: Hash, data: FaceData): (Global, Effect) = {
      def faceId(idx: Int, faceInfo: FaceInfo): FaceId = FaceId(hash, idx) //, faceInfo.rectangle)
      def face(idx: Int, faceInfo: FaceInfo): PerFace = PerFace(faceId(idx, faceInfo), FaceUtils.createVector(faceInfo.modelData), Vector.empty)
      val allFaces = data.faces.zipWithIndex.map { case (info, idx) => face(idx, info) }

      val newVectors = vectors ++ allFaces.map(f => (f.vector, f.faceId))

      def injectFace(p: PerFace): Effect = {
        val neighbors =
          vectors
            .map(e => e._2 -> FaceUtils.sqdist(e._1, p.vector, effectiveThreshold))
            .filter(_._2 < effectiveThreshold)

        val newFace = p.copy(neighbors = neighbors)
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
      neighbors: Vector[(FaceId, Int)]
  ) {
    def addNeighbor(f: FaceId, distance: Int): PerFace = copy(neighbors = neighbors :+ (f -> distance))
  }
  /*case class PerHash(
      faces: Vector[PerFace]
  ) {
    def similarFacesTo(idx: Int): Vector[(Hash, Int, Float)] =
      faces.lift(idx).fold(Vector.empty[(Hash, Int, Float)])(_.neighbors.map { case (FaceId(hash, idx), dist) => (hash, idx, dist.toFloat / FaceUtils.Factor / FaceUtils.Factor) })
    def addNeighbor(idx: Int, neighbor: FaceId, dist: Int): PerHash = copy(faces = faces.updated(idx, faces(idx).addNeighbor(neighbor, dist)))
  }*/

  def version: Int = 1
  def initialGlobalState: Global = Global(Vector.empty)
  def initialPerKeyState(key: FaceId): PerFace = PerFace(key, Array.empty, Vector.empty) // FIXME? Or needed at all? PerHash(Vector.empty)
  def processEvent(event: MetadataEnvelope): Effect =
    event.entry.kind match {
      case FaceData => Effect.flatMapGlobalState(_.handle(event.entry.target.hash, event.entry.value.asInstanceOf[FaceData]))
      case _        => Effect.Empty
    }

  def api(handleWithState: PerKeyHandleWithStateFunc[FaceId, PerFace])(implicit ec: ExecutionContext): SimilarFaces =
    new SimilarFaces {
      def similarFacesTo(hash: Hash, idx: Int): Future[Vector[(Hash, Int, Float)]] =
        handleWithState.access(FaceId(hash, idx))(_.neighbors.map { case (FaceId(hash, idx), dist) => (hash, idx, dist.toFloat / FaceUtils.Factor / FaceUtils.Factor) })
    }
  import spray.json._
  import DefaultJsonProtocol._
  implicit val faceIdFormat: JsonFormat[FaceId] = jsonFormat2(FaceId)
  def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[PerFace] = jsonFormat3(PerFace)
  def globalStateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[Global] =
    jsonFormat1(Global.apply _)

  override def deserializeKey(keyString: String): FaceId = {
    val Array(hash, idx) = keyString.split("#")
    FaceId(Hash.fromPrefixedString(hash).get, idx.toInt)
  }
  override def serializeKey(key: FaceId): String = s"${key.hash.toString}#${key.idx}"

  def hasWork(key: Key, state: PerFace): Boolean = false
  def createWork(key: Key, state: PerFace, context: ExtractionContext): (PerFace, Vector[WorkEntry]) = (state, Vector.empty)
}

object FaceUtils {
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
