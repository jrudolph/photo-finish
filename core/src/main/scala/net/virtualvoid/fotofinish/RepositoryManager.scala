package net.virtualvoid.fotofinish

import net.virtualvoid.fotofinish.metadata.{ IngestionData, IngestionDataExtractor, Metadata, MetadataEntry, MetadataStore }

final case class FileAndMetadata(fileInfo: FileInfo, metadata: Metadata)
class RepositoryManager(val config: RepositoryConfig) {
  val FileNamePattern = """^[0-9a-f]{128}$""".r
  import Scanner._

  def metadataFor(hash: Hash): Metadata =
    MetadataStore.load(config.fileInfoOf(hash))

  def scanAllRepoFiles(): Iterator[FileInfo] =
    Scanner.allFilesMatching(config.primaryStorageDir, byFileName(str => FileNamePattern.findFirstMatchIn(str).isDefined))
      .iterator
      .map(f => Hash.fromString(config.hashAlgorithm, f.getName))
      .map(config.fileInfoOf)

  def allRepoFiles(): Iterator[FileInfo] = _allRepoFiles.iterator

  def allFiles(): Iterator[FileAndMetadata] =
    allRepoFiles()
      .map { fileInfo =>
        FileAndMetadata(fileInfo, MetadataStore.load(fileInfo))
      }

  lazy val ingestionEntries: Seq[MetadataEntry[IngestionData]] = {
    println("Loading all ingestion data...")
    val res =
      MetadataStore.loadAllEntriesFrom(config.metadataCollectionFor(IngestionDataExtractor))
        .entries.asInstanceOf[Seq[MetadataEntry[IngestionData]]]
    println("Done")
    res
  }

  private lazy val _allRepoFiles: Vector[FileInfo] = {
    val entries = ingestionEntries // initializing
    println("Sorting entries...")
    val res =
      entries.map(_.header.forData).distinct
        .sorted
        .map(config.fileInfoOf)
        .toVector
    println("Done")
    res
  }

  lazy val inodeMap: Map[(Long, Long), FileInfo] =
    scanAllRepoFiles()
      .map { info =>
        val UnixFileInfo(dev, ino) = Scanner.unixFileInfo(info.repoFile.toPath)

        (dev, ino) -> info
      }.toMap
}