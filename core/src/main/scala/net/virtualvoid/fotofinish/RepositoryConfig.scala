package net.virtualvoid.fotofinish

import java.io.File

case class RepositoryConfig(
    storageDir:    File,
    hashAlgorithm: HashAlgorithm
) {
  def primaryStorageDir: File = new File(storageDir, s"by-${hashAlgorithm.name}")

  def repoFile(hash: Hash): File = {
    val fileName = s"by-${hash.hashAlgorithm.name}/${hash.asHexString.take(2)}/${hash.asHexString}"
    new File(storageDir, fileName)
  }
  def metadataFile(hash: Hash): File = {
    val fileName = s"by-${hash.hashAlgorithm.name}/${hash.asHexString.take(2)}/${hash.asHexString}.metadata.json.gz"
    new File(storageDir, fileName)
  }

  def fileInfoOf(hash: Hash): FileInfo =
    FileInfo(
      hash,
      repoFile(hash),
      metadataFile(hash),
      repoFile(hash)
    )

  def fileInfoByHashPrefix(prefix: String, hashAlgorithm: HashAlgorithm = hashAlgorithm): Option[FileInfo] = {
    require(prefix.size > 2)

    val dir = new File(storageDir, s"by-${hashAlgorithm.name}/${prefix.take(2)}/")
    import Scanner._
    dir.listFiles(byFileName(name => name.startsWith(prefix) && name.length == hashAlgorithm.hexStringLength))
      .headOption
      .map(f => fileInfoOf(Hash.fromString(hashAlgorithm, f.getName)))
  }

}