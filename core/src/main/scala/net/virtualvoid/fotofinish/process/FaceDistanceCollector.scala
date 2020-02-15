package net.virtualvoid.fotofinish.process

import net.virtualvoid.fotofinish.Hash
import net.virtualvoid.fotofinish.metadata.{ ExtractionContext, FaceData, FaceInfo, MetadataEntry, MetadataEnvelope, Rectangle }
import spray.json.JsonFormat

import scala.concurrent.ExecutionContext

class FaceDistanceCollector(threshold: Float) extends LineBasedJsonSnaphotProcess {
  case class FaceId(
      hash:      Hash,
      idx:       Int,
      rectangle: Rectangle
  )
  case class PerFace(
      faceId:    FaceId,
      info:      FaceInfo,
      neighbors: Vector[(FaceId, Float)]
  ) {
    def addNeighbor(f: FaceId, distance: Float): PerFace = copy(neighbors = neighbors :+ (f -> distance))
  }
  case class State(entries: Map[FaceId, PerFace]) {
    def handle(hash: Hash, data: FaceData): State = {
      def faceId(idx: Int, faceInfo: FaceInfo): FaceId = FaceId(hash, idx, faceInfo.rectangle)
      def face(idx: Int, faceInfo: FaceInfo): PerFace = PerFace(faceId(idx, faceInfo), faceInfo, Vector.empty)

      val allFaces = data.faces.zipWithIndex.map { case (info, idx) => face(idx, info) }

      allFaces.foldLeft(this)(_.inject(_))
    }

    def inject(perFace: PerFace): State = {
      val neighbors =
        entries
          .values
          .map(e => e -> FaceUtils.sqdistFloat(e.info.modelData, perFace.info.modelData, threshold))
          .filter(_._2 < threshold)

      neighbors.foldLeft(addFace(perFace))((state, f) => state.addNeighbor(f._1.faceId, perFace.faceId, f._2))
    }
    def addFace(perFace: PerFace): State = copy(entries = entries + (perFace.faceId -> perFace))
    def update(face: FaceId, f: PerFace => PerFace): State = copy(entries = entries + (face -> f(entries(face))))
    def addNeighbor(f1: FaceId, f2: FaceId, distance: Float): State =
      update(f1, _.addNeighbor(f2, distance))
        .update(f2, _.addNeighbor(f1, distance))
  }

  type S = State
  type Api = Unit

  def version: Int = 1
  def initialState: State = State(Map.empty)
  def processEvent(state: State, event: MetadataEnvelope): State = event.entry.kind match {
    case FaceData => state.handle(event.entry.target.hash, event.entry.value.asInstanceOf[FaceData])
    case _        => state
  }
  def createWork(state: State, context: ExtractionContext): (State, Vector[WorkEntry]) = (state, Vector.empty)
  def api(handleWithState: HandleWithStateFunc[State])(implicit ec: ExecutionContext): Unit = ()

  type StateEntryT = (FaceId, PerFace)
  def stateAsEntries(state: State): Iterator[StateEntryT] = state.entries.iterator
  def entriesAsState(entries: Iterable[StateEntryT]): State = State(Map(entries.toVector: _*))
  def stateEntryFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[StateEntryT] = {
    import spray.json._
    import DefaultJsonProtocol._
    implicit val faceIdFormat: JsonFormat[FaceId] = jsonFormat3(FaceId)
    implicit val perFaceFormat: JsonFormat[PerFace] = jsonFormat3(PerFace)
    implicitly[JsonFormat[StateEntryT]]
  }
}

object FaceUtils {
  type FeatureVector = Array[Float]
  def sqdistFloat(a1: FeatureVector, a2: FeatureVector, threshold: Float): Float = {
    /* naive but slow because of boxing algorithm
    (a1, a2).zipped
      .map {
        case (a, b) =>
          val diff = a - b
          diff * diff
      }.sum
     */

    var i = 0
    var sqDiff = 0f
    while (i < a1.length && sqDiff < threshold) {
      val diff = a1(i) - a2(i)
      sqDiff += diff * diff

      i += 1
    }
    sqDiff
  }
}