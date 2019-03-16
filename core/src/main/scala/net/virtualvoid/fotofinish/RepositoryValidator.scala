package net.virtualvoid.fotofinish

import java.util.concurrent.atomic.AtomicInteger

import net.virtualvoid.fotofinish.metadata.MetadataStore

import scala.util.Try

object RepositoryValidator extends App {
  // Things to test:
  //   * Hashes are correct (takes a long time)
  //   * All per-repo-file metadata can be read and is for the given file
  //   * Aggregated metadata files contain same info as per-repo-files

  def checkHash(info: FileInfo): Try[Unit] = Try {
    val actualHash = info.hash.hashAlgorithm(info.repoFile)
    require(actualHash == info.hash, s"${info.hash.hashAlgorithm.name} for [${info.repoFile}] was expected to be [${info.hash}] but was [$actualHash]")
  }
  def checkMetadata(info: FileInfo): Try[Unit] = Try {
    val entries = MetadataStore.loadAllEntriesFrom(info.metadataFile)
    require(entries.entries.forall(_.header.forData == info.hash), s"Metadata file [${info.metadataFile}] for [${info.hash.asHexString}] contains entries for other hashes")
  }

  runCheck("Per-file metadata", checkMetadata)
  runCheck("Validating file hashes", checkHash)

  def runCheck(what: String, check: FileInfo => Try[Unit]): Unit =
    Settings.manager.allRepoFiles()
      .map(check)
      .map {
        val i = new AtomicInteger()

        r => {
          val at = i.incrementAndGet()
          if ((at % 500) == 0) println(s"[$what] $at")
          r
        }
      }
      .filter(_.isFailure)
      .foreach(println)
}
