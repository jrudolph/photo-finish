package net.virtualvoid.fotofinish

import java.io.File

import net.virtualvoid.fotofinish.metadata.{ ExifBaseData, FaceData, IngestionData, MetadataKind, MetadataManager, Thumbnail }

object Settings {
  /*val repo = new File("/home/johannes/Fotos/tmp/repo")
  val meta = new File("/home/johannes/Fotos/tmp/repo/metadata")
  val linkDir = new File("/home/johannes/FotosSorted")*/
  val repo = new File("/home/johannes/git/self/photo-finish/tmprepo/objects")
  val meta = new File("/home/johannes/git/self/photo-finish/tmprepo/metadata")
  val linkDir = new File("/home/johannes/git/self/photo-finish/tmprepo/links")

  val knownMetadataKinds = Set[MetadataKind](
    IngestionData,
    ExifBaseData,
    Thumbnail,
    FaceData,
  )

  val config =
    RepositoryConfig(
      repo,
      meta,
      linkDir,
      HashAlgorithm.Sha512,
      knownMetadataKinds)

  val manager = new RepositoryManager(config)
  val metadataStore = new MetadataManager(manager)
}
