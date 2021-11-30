package net.virtualvoid.fotofinish.metadata

import net.virtualvoid.fotofinish.util.JsonExtra._

import spray.json._

sealed trait TagEvent
case class TagAdded(tag: String) extends TagEvent
case class TagRemoved(tag: String) extends TagEvent
case class TagData(tagEvents: Seq[TagEvent])
object TagData extends MetadataKind.Impl[TagData]("net.virtualvoid.fotofinish.metadata.TagData", 1) {
  import spray.json.DefaultJsonProtocol._

  private implicit val tagAddedFormat: JsonFormat[TagAdded] = jsonFormat1(TagAdded.apply _)
  private implicit val tagRemovedFormat: JsonFormat[TagRemoved] = jsonFormat1(TagRemoved.apply _)
  private implicit val eventFormat: JsonFormat[TagEvent] = new JsonFormat[TagEvent] {
    override def read(json: JsValue): TagEvent = json.asJsObject.field("type") match {
      case JsString("TagAdded")   => json.convertTo[TagAdded]
      case JsString("TagRemoved") => json.convertTo[TagRemoved]
      case x                      => MetadataJsonProtocol.error(s"Cannot read TagEvent from $x")
    }
    override def write(obj: TagEvent): JsValue = obj match {
      case t: TagAdded   => t.toJson + ("type" -> JsString("TagAdded"))
      case t: TagRemoved => t.toJson + ("type" -> JsString("TagRemoved"))
    }
  }

  override implicit val jsonFormat: JsonFormat[TagData] = jsonFormat1(TagData.apply _)
}