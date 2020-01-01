package net.virtualvoid.fotofinish

import java.io.File

import net.virtualvoid.fotofinish.metadata.Id
import net.virtualvoid.fotofinish.metadata.Id.Hashed

case class FileInfo(
    hash:         Hash,
    repoFile:     File,
    originalFile: Option[File] // only set if file was just injected (TODO: could we do without putting it in here?)
) {
  def id: Id = Hashed(hash)
}