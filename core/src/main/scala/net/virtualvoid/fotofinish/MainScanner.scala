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
    val infos = new Scanner(repoConfig, manager).scan(dir)

    println("Updating basic metadata for ingested fotos")
    infos.par.foreach(MetadataStore.updateMetadataFor(_, IngestionDataExtractor, repoConfig))

    println("Updating exif metadata for ingested fotos")
    infos.par.foreach(MetadataStore.updateMetadataFor(_, ExifBaseDataExtractor, repoConfig))

    println("Updating all metadata for ingested fotos")
    infos.par.foreach(MetadataStore.updateMetadata(_, repoConfig))

    infos
  }
  dirs.flatMap(ingestDir)

  println("Updating remaining metadata")
  manager.allRepoFiles().toStream.par
    .foreach(MetadataStore.updateMetadata(_, repoConfig))

  println("Updating by-date folder")
  Relinker.createDirStructure(manager)(Relinker.byYearMonth(manager))

  println("Updating by-original-name folder")
  Relinker.createDirStructure(manager)(Relinker.byOriginalFileName(manager))
}

