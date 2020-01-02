package net.virtualvoid.fotofinish

import java.io.File

import net.virtualvoid.fotofinish.metadata.{ ExifBaseData, ExifBaseDataExtractor, FaceData, FaceDataExtractor, FileTypeData, FileTypeDataExtractor, IngestionData, MetadataExtractor, MetadataKind, Thumbnail, ThumbnailExtractor }

object Settings {
  /*val repo = new File("/home/johannes/Fotos/tmp/repo")
  val meta = new File("/home/johannes/Fotos/tmp/repo/metadata")
  val linkDir = new File("/home/johannes/FotosSorted")*/
  val repo = new File("/home/johannes/git/self/photo-finish/tmprepo/objects")
  val meta = new File("/home/johannes/git/self/photo-finish/tmprepo/metadata")
  val linkDir = new File("/home/johannes/git/self/photo-finish/tmprepo/links")

  val knownMetadataKinds = Set[MetadataKind](
    IngestionData,
    FileTypeData,
    ExifBaseData,
    Thumbnail,
    FaceData,
  )

  val autoExtractors: Set[MetadataExtractor] = Set(
    FileTypeDataExtractor,
    ExifBaseDataExtractor,
    ThumbnailExtractor,
  //FaceDataExtractor,
  )

  val config =
    RepositoryConfig(
      repo,
      meta,
      linkDir,
      HashAlgorithm.Sha512,
      knownMetadataKinds,
      autoExtractors,
      8)

  val manager = new RepositoryManager(config)
}
