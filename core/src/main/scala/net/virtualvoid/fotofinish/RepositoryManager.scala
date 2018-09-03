package net.virtualvoid.fotofinish

import net.virtualvoid.fotofinish.metadata.Metadata
import net.virtualvoid.fotofinish.metadata.MetadataStore

final case class FileAndMetadata(fileInfo: FileInfo, metadata: Metadata)
class RepositoryManager(val config: RepositoryConfig) {
  val FileNamePattern = """^[0-9a-f]{128}$""".r
  import Scanner._

  def metadataFor(hash: Hash): Metadata =
    MetadataStore.load(config.fileInfoOf(hash))

  def allRepoFiles(): Iterator[FileInfo] =
    Scanner.allFilesMatching(config.primaryStorageDir, byFileName(str => FileNamePattern.findFirstMatchIn(str).isDefined))
      .iterator
      .map(f => Hash.fromString(config.hashAlgorithm, f.getName))
      .map(config.fileInfoOf)

  def allFiles(): Iterator[FileAndMetadata] =
    allRepoFiles()
      .map { fileInfo =>
        FileAndMetadata(fileInfo, MetadataStore.load(fileInfo))
      }

  lazy val inodeMap: Map[(Long, Long), FileInfo] =
    allRepoFiles()
      .map { info =>
        val UnixFileInfo(dev, ino) = Scanner.unixFileInfo(info.repoFile.toPath)

        (dev, ino) -> info
      }.toMap
}