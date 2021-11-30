package net.virtualvoid.fotofinish.metadata
import spray.json.JsonFormat

case class RatingData(stars: Int, comment: String) {
  require(stars >= 0 && stars <= 5, s"Stars must be between 0 and 5 but was $stars")
}
object RatingData extends MetadataKind.Impl[RatingData]("net.virtualvoid.fotofinish.metadata.RatingData", 1) {
  import spray.json.DefaultJsonProtocol._
  override implicit def jsonFormat: JsonFormat[RatingData] = jsonFormat2(RatingData.apply _)
}
