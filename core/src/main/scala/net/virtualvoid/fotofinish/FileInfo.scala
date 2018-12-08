package net.virtualvoid.fotofinish

import java.io.File

case class FileInfo(
    hash:         Hash,
    repoFile:     File,
    metadataFile: File,
    originalFile: Option[File] // only set if file was just injected (TODO: could we do without putting it in here?)
)