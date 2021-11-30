package net.virtualvoid.fotofinish.metadata
import spray.json.JsonFormat

case class UserData(name: String)
object UserData extends MetadataKind.Impl[UserData]("net.virtualvoid.fotofinish.metadata.UserData", 1) {
  import spray.json.DefaultJsonProtocol._
  override implicit val jsonFormat: JsonFormat[UserData] = jsonFormat1(UserData.apply)
}