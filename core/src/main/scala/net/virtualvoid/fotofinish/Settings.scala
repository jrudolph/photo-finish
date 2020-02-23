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
  //val cacheDir = new File(meta, "cache")
  val cacheDir = new File("/tmp/cache")

  repo.mkdirs()
  meta.mkdirs()
  cacheDir.mkdirs()

  val knownMetadataKinds = Set[MetadataKind](
    IngestionData,
    FileTypeData,
    ExifBaseData,
    Thumbnail,
    FaceData,
    HashData,
    FFProbeData,
  )

  val autoExtractors: Set[MetadataExtractor] = Set(
    FileTypeDataExtractor.instance,
    ExifBaseDataExtractor.instance,
    ThumbnailExtractor.instance,
    HashDataExtractor.instance,
    FFProbeDataExtractor.instance,
  //FaceDataExtractor.instance,
  )

  val config =
    RepositoryConfig(
      repo,
      meta,
      linkDir,
      cacheDir,
      HashAlgorithm.Sha512T160,
      knownMetadataKinds,
      autoExtractors,
      8,
      30.seconds,
      0
    )

  val scanner = new Scanner(config)
}
