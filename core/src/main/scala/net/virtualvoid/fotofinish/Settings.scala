package net.virtualvoid.fotofinish

import java.io.File

object Settings {
  val repo = new File("/home/johannes/Fotos/tmp/repo")

  val repoConfig = RepositoryConfig(repo, HashAlgorithm.Sha512)
  val manager = new RepositoryManager(repoConfig)
}
