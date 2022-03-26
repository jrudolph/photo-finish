package net.virtualvoid.fotofinish

import java.io.{ File, FileOutputStream }
import net.virtualvoid.fotofinish.metadata._
import net.virtualvoid.fotofinish.process.{ JournalConfig, ProcessConfig }

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

final case class RepositoryConfig(
    storageDir:          File,
    metadataDir:         File,
    linkRootDir:         File,
    cacheDir:            File,
    hashAlgorithm:       HashAlgorithm,
    knownMetadataKinds:  Set[MetadataKind],
    metadataMapper:      MetadataEntry => MetadataEntry,
    autoExtractors:      Set[MetadataExtractor],
    executorParallelism: Int,
    snapshotInterval:    FiniteDuration,
    snapshotOffset:      Long // only create a new snapshot if this many events have been processed since (because replaying will likely be faster otherwise)
) extends ProcessConfig with JournalConfig with EntryFormatsFromKinds {
  val primaryStorageDir: File = new File(storageDir, s"by-${hashAlgorithm.name}")
  val allMetadataFile: File = new File(metadataDir, "metadata.json.gz")

  val snapshotDir: File = new File(cacheDir, "snapshots")
  snapshotDir.mkdirs()

  {
    val cacheTag = new File(cacheDir, "CACHEDIR.TAG")
    if (!cacheTag.exists()) Try {
      val fos = new FileOutputStream(cacheTag)
      fos.write("Signature: 8a477f597d28d172789f06886806bc55\n# This file is a cache directory tag created by photo-finish.\n# For information about cache directory tags, see:\n#\thttp://www.brynosaurus.com/cachedir/".getBytes("ASCII"))
      fos.close()
    }
  }

  val metadataIndexFile: File = new File(cacheDir, "metadata.json.gz.idx")

  def metadataCollectionFor(kind: MetadataKind): File = new File(metadataDir, s"${kind.kind}-v${kind.version}.json.gz")

  override def repoFileFor(hash: Hash): File = repoFile(hash)
  def repoFile(hash: Hash): File = {
    val fileName = s"by-${hash.hashAlgorithm.name}/${hash.asHexString.take(2)}/${hash.asHexString}"
    val file0 = new File(storageDir, fileName)
    if (!file0.exists && hash.hashAlgorithm.underlying.isDefined)
      fileInfoByHashPrefix(hash.asHexString, hash.hashAlgorithm.underlying.get)
        .fold(file0)(_.repoFile)
    else
      file0
  }
  def metadataFile(id: Id): File = metadataFile(id.hash)
  def metadataFile(hash: Hash): File = {
    val fileName = s"by-${hash.hashAlgorithm.name}/${hash.asHexString.take(2)}/${hash.asHexString}.metadata.json.gz"
    new File(storageDir, fileName)
  }

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
    if (dir.exists())
      dir.listFiles(byFileName(name => name.startsWith(prefix) && name.length == hashAlgorithm.hexStringLength))
        .headOption
        .map(f => fileInfoOf(Hash.fromString(hashAlgorithm, f.getName)))
    else None
  }

  def destinationsFor(entry: MetadataEntry): Seq[File] =
    metadataFile(entry.target) +: centralDestinationsFor(entry)

  def centralDestinationsFor(entry: MetadataEntry): Seq[File] =
    Seq(
      allMetadataFile,
      metadataCollectionFor(entry.kind)
    )

  override protected def allKinds: Set[MetadataKind] = knownMetadataKinds
}