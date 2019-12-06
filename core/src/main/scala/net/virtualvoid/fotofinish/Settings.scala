package net.virtualvoid.fotofinish

import java.io.File

import net.virtualvoid.fotofinish.metadata.MetadataStore

object Settings {
  val repo = new File("/home/johannes/Fotos/tmp/repo")
  val meta = new File("/home/johannes/Fotos/tmp/repo/metadata")
  val linkDir = new File("/home/johannes/FotosSorted")

  val repoConfig =
    RepositoryConfig(
      repo,
      meta,
      linkDir,
      HashAlgorithm.Sha512)
  val manager = new RepositoryManager(repoConfig)
  val metadataStore = new MetadataStore(repoConfig)
}
