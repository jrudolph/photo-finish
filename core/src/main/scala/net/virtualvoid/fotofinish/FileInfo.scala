package net.virtualvoid.fotofinish

import java.io.File

case class FileInfo(
    hash:         Hash,
    repoFile:     File,
    metadataFile: File,
    originalFile: File
)