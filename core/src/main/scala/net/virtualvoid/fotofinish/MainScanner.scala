package net.virtualvoid.fotofinish

import java.io.File

import net.virtualvoid.fotofinish.metadata._

import scala.collection.immutable

object MainScanner extends App {

  import Settings._

  val dirs =
    {
      (2003 to 2020).map(year => s"/home/johannes/Fotos/$year") ++
        Seq[String]()
    }
      .map(new File(_))
      .filter(_.exists)

  def ingestDir(dir: File): immutable.Seq[FileInfo] = {
    println(s"Ingesting new files from $dir")
    val is = new Scanner(config, manager).scan(dir)
    // HACK:
    // metadata access isn't safely concurrently accessible so make sure not to run analyses in parallel per repo file/hash
    val infos = is.groupBy(_.hash.asHexString)

    println("Updating basic metadata for ingested photos")
    infos.par.foreach(i => i._2.foreach(metadataStore.updateMetadataFor(_, IngestionDataExtractor)))

    println("Updating exif metadata for ingested photos")
    infos.par.foreach(i => i._2.foreach(metadataStore.updateMetadataFor(_, ExifBaseDataExtractor)))

    println("Updating all metadata for ingested photos")
    infos.par.foreach(i => i._2.foreach(metadataStore.updateMetadata))

    is
  }
  dirs.flatMap(ingestDir)

  println("Updating remaining metadata")
  manager.allRepoFiles().toStream.par
    .foreach(metadataStore.updateMetadata)

  println("Updating by-date folder")
  Relinker.createDirStructure(manager)(Relinker.byYearMonth(manager))

  println("Updating by-original-name folder")
  Relinker.createDirStructure(manager)(Relinker.byOriginalFileName(manager))
}

