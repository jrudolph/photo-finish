package net.virtualvoid.metadata

final case class Instant(millisSinceEpoch: Long) extends AnyVal

final case class Hash(hashAlgorithm: String, hash: String)

sealed trait DataId
final case class ContentAddressed(hash: Hash)

sealed trait MetadataCreatorKind
object MetadataCreatorKind {
  case object Automatic extends MetadataCreatorKind
}

final case class MetadataCreator(
    kind:    MetadataCreatorKind,
    name:    String,
    version: String,
    machine: String
)

final case class MetadataRepositoryId(name: String)

/**
 *
 * @param dataId Data that is described by this metadata.
 * @param createdBy The creator of this metadata. This can be a program or a person or another actor.
 * @param repository The repository this metadata belongs to.
 * @param sequenceNo The sequence number uniquely identifying this piece of metadata in the repo.
 * @param createdAt Time this metadata was entered into the repository.
 * @param kind The kind of metadata.
 * @param version The version of this metadata kind.
 */
final case class MetadataHeader(
    dataId:     DataId,
    createdBy:  MetadataCreator,
    repository: MetadataRepositoryId,
    sequenceNo: Long,
    createdAt:  Instant,
    kind:       String,
    version:    Int
)