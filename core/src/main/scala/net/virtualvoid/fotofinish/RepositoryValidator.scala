package net.virtualvoid.fotofinish

import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ Await, Future }
import scala.util.Try

object RepositoryValidator extends App {
  implicit val system = ActorSystem()
  import system.dispatcher
  // Things to test:
  //   * Hashes are correct (takes a long time)
  //   * All per-repo-file metadata can be read and is for the given file
  //   * Aggregated metadata files contain same info as per-repo-files
  //   * Sequence numbers are consecutive

  def checkHash(info: FileInfo): Try[Unit] = Try {
    val actualHash = info.hash.hashAlgorithm(info.repoFile)
    require(actualHash == info.hash, s"${info.hash.hashAlgorithm.name} for [${info.repoFile}] was expected to be [${info.hash}] but was [$actualHash]")
  }
  /*def checkMetadata(info: FileInfo): Try[Unit] = Try {
    val metadataFile = Settings.config.metadataFile(info.hash)
    val entries = MetadataManager.loadAllEntriesFrom(metadataFile)
    require(
      entries.entries.forall(_.header.forData == info.hash),
      s"Metadata file [$metadataFile] for [${info.hash.asHexString}] contains entries for other hashes")
  }*/

  println("Scanning repository")
  val map = Settings.scanner.inodeMap

  println("Sorting by inode")
  val sorted = map.toVector.sortBy(_._1).map(_._2)

  //runCheck("Per-file metadata", checkMetadata)
  import scala.concurrent.duration._
  Await.result(runCheck("Validating file hashes", checkHash), 30.minutes)
  Await.result(system.terminate(), 1.minute)

  def runCheck(what: String, check: FileInfo => Try[Unit]): Future[Done] = {
    println(s"Starting check [$what]")
    Source(sorted)
      .mapAsyncUnordered(16)(fi => Future(check(fi)))
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
      .runForeach(println)
  }
}
