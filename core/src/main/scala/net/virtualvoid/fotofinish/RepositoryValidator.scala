package net.virtualvoid.fotofinish

import java.util.concurrent.atomic.AtomicInteger

import net.virtualvoid.fotofinish.metadata.MetadataManager

import scala.util.Try

object RepositoryValidator extends App {
  // Things to test:
  //   * Hashes are correct (takes a long time)
  //   * All per-repo-file metadata can be read and is for the given file
  //   * Aggregated metadata files contain same info as per-repo-files
  //   * Sequence numbers are consecutive

  def checkHash(info: FileInfo): Try[Unit] = Try {
    val actualHash = info.hash.hashAlgorithm(info.repoFile)
    require(actualHash == info.hash, s"${info.hash.hashAlgorithm.name} for [${info.repoFile}] was expected to be [${info.hash}] but was [$actualHash]")
  }
  def checkMetadata(info: FileInfo): Try[Unit] = Try {
    val metadataFile = Settings.config.metadataFile(info.hash)
    val entries = MetadataManager.loadAllEntriesFrom(metadataFile)
    require(
      entries.entries.forall(_.header.forData == info.hash),
      s"Metadata file [$metadataFile] for [${info.hash.asHexString}] contains entries for other hashes")
  }

  println("Scanning repository")
  val map = Settings.manager.inodeMap

  println("Sorting by inode")
  val sorted = map.toVector.sortBy(_._1).map(_._2)

  runCheck("Per-file metadata", checkMetadata)
  runCheck("Validating file hashes", checkHash)

  def runCheck(what: String, check: FileInfo => Try[Unit]): Unit = {
    println(s"Starting check [$what]")
    sorted.toIterator
      .map(check)
      .map {
        val i = new AtomicInteger()
        val started = System.nanoTime()
        @volatile var startedBatchAt = started
        val batch = 500

        r => {
          val at = i.incrementAndGet()
          if ((at % batch) == 0) {
            val now = System.nanoTime()
            val millisPerElem = (now - started) / at / 1000000
            val remainingSeconds = millisPerElem * (sorted.size - at) / 1000
            println(f"[$what] $at%6d/${sorted.size}%6d ETA: ${remainingSeconds / 60}%02d:${remainingSeconds % 60}%02d")

            startedBatchAt = now
          }
          r
        }
      }
      .filter(_.isFailure)
      .foreach(println)
  }
}
