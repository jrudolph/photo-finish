package net.virtualvoid.fotofinish

import java.io.File

import scala.concurrent.duration._

import net.virtualvoid.fotofinish.metadata._

object Settings {
  /*val repo = new File("/home/johannes/Fotos/tmp/repo")
  val meta = new File("/home/johannes/Fotos/tmp/repo/metadata")
  val linkDir = new File("/home/johannes/FotosSorted")*/
  val repo = new File("/home/johannes/git/self/photo-finish/tmprepo/objects")
  val meta = new File("/home/johannes/git/self/photo-finish/tmprepo/metadata")
  val linkDir = new File("/home/johannes/git/self/photo-finish/tmprepo/links")

  repo.mkdirs()
  meta.mkdirs()

  val knownMetadataKinds = Set[MetadataKind](
    IngestionData,
    FileTypeData,
    ExifBaseData,
    Thumbnail,
    FaceData,
    HashData,
  )

  val autoExtractors: Set[MetadataExtractor] = Set(
    FileTypeDataExtractor.instance,
    ExifBaseDataExtractor.instance,
    ThumbnailExtractor.instance,
    HashDataExtractor.instance,
  //FaceDataExtractor.instance,
  )

  val config =
    RepositoryConfig(
      repo,
      meta,
      linkDir,
      HashAlgorithm.Sha512T160,
      knownMetadataKinds,
      autoExtractors,
      8,
      30.seconds,
      0
    )

  val scanner = new Scanner(config)
}
