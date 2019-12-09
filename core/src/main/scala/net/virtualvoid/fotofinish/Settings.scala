package net.virtualvoid.fotofinish

import java.io.File

import net.virtualvoid.fotofinish.metadata.MetadataManager

object Settings {
  val repo = new File("/home/johannes/Fotos/tmp/repo")
  val meta = new File("/home/johannes/Fotos/tmp/repo/metadata")
  val linkDir = new File("/home/johannes/FotosSorted")

  val config =
    RepositoryConfig(
      repo,
      meta,
      linkDir,
      HashAlgorithm.Sha512)
  val manager = new RepositoryManager(config)
  val metadataStore = new MetadataManager(config)
}
