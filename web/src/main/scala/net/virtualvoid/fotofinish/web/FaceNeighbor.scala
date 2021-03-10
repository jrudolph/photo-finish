package net.virtualvoid.fotofinish.web

import akka.http.scaladsl.model.DateTime
import net.virtualvoid.fotofinish.Hash

case class FaceNeighbor(targetHash: Hash, faceIdx: Int, distance: Float, title: String, dateTaken: Option[DateTime]) {
  def dateTakenOrStartOfEpoch: DateTime = dateTaken.getOrElse(DateTime(0L))
}
