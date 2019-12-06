package net.virtualvoid.fotofinish

import java.io.File

import net.virtualvoid.fotofinish.metadata.{ MetadataEntry, MetadataExtractor }

final case class RepositoryConfig(
    storageDir:    File,
    metadataDir:   File,
    linkRootDir:   File,
    hashAlgorithm: HashAlgorithm
) {
  val primaryStorageDir: File = new File(storageDir, s"by-${hashAlgorithm.name}")
  val allMetadataFile: File = new File(metadataDir, "metadata.json.gz")
  def metadataCollectionFor(extractor: MetadataExtractor): File = new File(metadataDir, s"${extractor.kind}-v${extractor.version}.json.gz")

  def repoFile(hash: Hash): File = {
    val fileName = s"by-${hash.hashAlgorithm.name}/${hash.asHexString.take(2)}/${hash.asHexString}"
    new File(storageDir, fileName)
  }
  def metadataFile(hash: Hash): File = {
    val fileName = s"by-${hash.hashAlgorithm.name}/${hash.asHexString.take(2)}/${hash.asHexString}.metadata.json.gz"
    new File(storageDir, fileName)
  }

  def fileInfoOf(hash: Hash): FileInfo =
    FileInfo(
      hash,
      repoFile(hash),
      metadataFile(hash),
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

  def destinationsFor(entry: MetadataEntry[_]): Seq[File] =
    metadataFile(entry.header.forData) +: centralDestinationsFor(entry)

  def centralDestinationsFor(entry: MetadataEntry[_]): Seq[File] =
    Seq(
      allMetadataFile,
      metadataCollectionFor(entry.extractor)
    )
}