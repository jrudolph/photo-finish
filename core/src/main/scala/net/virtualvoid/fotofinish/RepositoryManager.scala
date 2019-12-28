package net.virtualvoid.fotofinish

import net.virtualvoid.fotofinish.metadata.{ IngestionData, Metadata, MetadataEntry2, MetadataEnvelope, MetadataManager }

class RepositoryManager(val config: RepositoryConfig) {
  val FileNamePattern = """^[0-9a-f]{128}$""".r
  import Scanner._

  def metadataFor(hash: Hash): Metadata = MetadataManager.loadAllEntriesFrom(config.metadataFile(hash))

  def scanAllRepoFiles(): Iterator[FileInfo] =
    Scanner.allFilesMatching(config.primaryStorageDir, byFileName(str => FileNamePattern.findFirstMatchIn(str).isDefined))
      .iterator
      .map(f => Hash.fromString(config.hashAlgorithm, f.getName))
      .map(config.fileInfoOf)

  def allRepoFiles(): Iterator[FileInfo] = _allRepoFiles.iterator

  lazy val ingestionEntries: Seq[MetadataEntry2.Aux[IngestionData]] = ??? /*{
    println("Loading all ingestion data...")
    val res =
      MetadataManager.loadAllEntriesFrom(config.metadataCollectionFor(IngestionDataExtractor))
        .entries.asInstanceOf[Seq[MetadataEntry2.Aux[IngestionData]]]
    println("Done")
    res
  }*/

  private lazy val _allRepoFiles: Vector[FileInfo] = ??? /*{
    val entries = ingestionEntries // initializing
    println("Sorting entries...")
    val res =
      entries.map(_.header.forData).distinct
        .sorted
        .map(config.fileInfoOf)
        .toVector
    println("Done")
    res
  }*/

  lazy val inodeMap: Map[(Long, Long), FileInfo] =
    scanAllRepoFiles()
      .map { info =>
        val UnixFileInfo(dev, ino) = Scanner.unixFileInfo(info.repoFile.toPath)

        (dev, ino) -> info
      }.toMap

  implicit val entryFormat = MetadataEntry2.entryFormat(config.knownMetadataKinds)
}