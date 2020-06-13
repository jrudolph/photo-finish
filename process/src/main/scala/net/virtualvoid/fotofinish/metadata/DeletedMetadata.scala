package net.virtualvoid.fotofinish.metadata

import net.virtualvoid.fotofinish.metadata.MetadataJsonProtocol.SimpleKind
import spray.json.JsonFormat

case class DeletedMetadata(
    reason:       String,
    previousKind: SimpleKind,
    filteredOut:  Boolean
)
object DeletedMetadata extends MetadataKind.Impl[DeletedMetadata]("net.virtualvoid.fotofinish.metadata", 1) {
  import spray.json.DefaultJsonProtocol._
  import MetadataJsonProtocol.simpleKindFormat
  override implicit def jsonFormat: JsonFormat[DeletedMetadata] = jsonFormat3(DeletedMetadata.apply _)
}