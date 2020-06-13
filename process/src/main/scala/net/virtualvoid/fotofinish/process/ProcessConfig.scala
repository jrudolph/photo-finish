package net.virtualvoid.fotofinish.process

import java.io.File

import net.virtualvoid.fotofinish.Hash
import net.virtualvoid.fotofinish.metadata.MetadataEntry
import spray.json.JsonFormat

import scala.concurrent.duration.FiniteDuration

trait ProcessConfig {
  def metadataDir: File
  def snapshotDir: File
  def allMetadataFile: File
  def metadataIndexFile: File
  def metadataMapper: MetadataEntry => MetadataEntry
  def snapshotOffset: Long
  def snapshotInterval: FiniteDuration
  def repoFileFor(hash: Hash): File
  implicit def entryFormat: JsonFormat[MetadataEntry]
}
