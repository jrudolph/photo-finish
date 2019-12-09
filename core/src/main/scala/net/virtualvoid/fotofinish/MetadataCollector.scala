package net.virtualvoid.fotofinish

import java.util.concurrent.atomic.AtomicLong

import metadata._

/** An app that collects all metadata from the central storage and repackages it to the central metadata store */
object MetadataCollector extends App {
  def loadAllMetadata(): Vector[MetadataEntry] = {
    import Settings._

    println("Scanning repository")
    val map = manager.inodeMap

    println("Sorting by inode")
    val sorted = map.toVector.sortBy(_._1).map(_._2)

    val read = new AtomicLong(0)

    println(s"Found ${sorted.size} files in repo")
    println("Collecting metadata")
    val entries =
      sorted
        .par
        .flatMap { fi =>
          val idx = read.incrementAndGet()
          if (idx % 1000 == 0) println(s"$idx/${sorted.size}")
          MetadataManager.loadAllEntriesFrom(config.metadataFile(fi.hash)).entries
        }
        .seq

    println(s"Found ${entries.size} entries")
    println(s"Sorting by creation time and hash")
    entries.sortBy(x => (x.header.created.clicks, x.header.forData.asHexString))
  }

  val entries = loadAllMetadata()
  println("Now storing data again")
  entries
    .zipWithIndex
    .foreach {
      case (e, idx) =>
        if (idx % 10000 == 0) println(s"$idx/${entries.size}")
        Settings.metadataStore.storeToDestinations(e, Settings.config.centralDestinationsFor(e))
    }
}
