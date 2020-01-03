package net.virtualvoid.fotofinish

import java.io.File

import net.virtualvoid.fotofinish.metadata.MetadataJsonProtocol.{ SimpleEntry, SimpleJournalEntry, SimpleKind }
import net.virtualvoid.fotofinish.metadata.{ Id, MetadataEntry, MetadataEnvelope, MetadataExtractor, MetadataKind }
import net.virtualvoid.fotofinish.util.JsonExtra
import spray.json.JsonFormat

import scala.concurrent.duration.FiniteDuration

final case class RepositoryConfig(
    storageDir:          File,
    metadataDir:         File,
    linkRootDir:         File,
    hashAlgorithm:       HashAlgorithm,
    knownMetadataKinds:  Set[MetadataKind],
    autoExtractors:      Set[MetadataExtractor],
    executorParallelism: Int,
    snapshotInterval:    FiniteDuration
) {
  val primaryStorageDir: File = new File(storageDir, s"by-${hashAlgorithm.name}")
  val allMetadataFile: File = new File(metadataDir, "metadata.json.gz")
  def metadataCollectionFor(kind: MetadataKind): File = new File(metadataDir, s"${kind.kind}-v${kind.version}.json.gz")

  def repoFile(hash: Hash): File = {
    val fileName = s"by-${hash.hashAlgorithm.name}/${hash.asHexString.take(2)}/${hash.asHexString}"
    new File(storageDir, fileName)
  }
  def metadataFile(id: Id): File = metadataFile(id.hash)
  def metadataFile(hash: Hash): File = {
    val fileName = s"by-${hash.hashAlgorithm.name}/${hash.asHexString.take(2)}/${hash.asHexString}.metadata.json.gz"
    new File(storageDir, fileName)
  }

  @deprecated // FIXME: Added for compatibility but should we really offer this?
  def fileInfoOf(id: Id): FileInfo = fileInfoOf(id.hash)
  def fileInfoOf(hash: Hash): FileInfo =
    FileInfo(
      hash,
      repoFile(hash),
      None
    )

  def fileInfoByHashPrefix(prefix: String, hashAlgorithm: HashAlgorithm = hashAlgorithm): Option[FileInfo] = {
    require(prefix.size > 2)

    val dir = new File(storageDir, s"by-${hashAlgorithm.name}/${prefix.take(2)}/")
    import Scanner._
    dir.listFiles(byFileName(name => name.startsWith(prefix) && name.length == hashAlgorithm.hexStringLength))
      .headOption
      .map(f => fileInfoOf(Hash.fromString(hashAlgorithm, f.getName)))
  }

  def destinationsFor(entry: MetadataEntry): Seq[File] =
    metadataFile(entry.target) +: centralDestinationsFor(entry)

  def centralDestinationsFor(entry: MetadataEntry): Seq[File] =
    Seq(
      allMetadataFile,
      metadataCollectionFor(entry.kind)
    )

  def resolve(kind: SimpleKind): MetadataKind =
    knownMetadataKinds.find(k => k.kind == kind.kind && k.version == kind.version)
      .getOrElse(throw new IllegalArgumentException(s"No MetadataKind found for [$kind] (has [${knownMetadataKinds.mkString(", ")}])"))
  def resolve(entry: SimpleEntry): MetadataEntry = {
    val kind = resolve(entry.kind)

    MetadataEntry[kind.T](
      entry.target,
      entry.secondaryTargets,
      kind,
      entry.creation,
      entry.value.convertTo[kind.T](kind.jsonFormat)
    )
  }
  def resolve(simpleJournalEntry: SimpleJournalEntry): MetadataEnvelope =
    MetadataEnvelope(simpleJournalEntry.seqNr, resolve(simpleJournalEntry.entry))

  import net.virtualvoid.fotofinish.metadata.MetadataJsonProtocol.simpleEntryFormat
  implicit def entryFormat: JsonFormat[MetadataEntry] =
    JsonExtra.deriveFormatFrom[SimpleEntry](SimpleEntry(_), resolve(_))
}