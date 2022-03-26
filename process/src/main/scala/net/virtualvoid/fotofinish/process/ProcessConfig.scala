package net.virtualvoid.fotofinish.process

import java.io.File
import net.virtualvoid.fotofinish.Hash
import net.virtualvoid.fotofinish.metadata.{ MetadataEntry, MetadataEnvelope }
import spray.json.JsonFormat

import scala.concurrent.duration.FiniteDuration

trait ProcessConfig {
  def snapshotDir: File
  def snapshotOffset: Long
  def snapshotInterval: FiniteDuration
  def repoFileFor(hash: Hash): File
  implicit def entryFormat: JsonFormat[MetadataEntry]
  implicit def envelopeFormat: JsonFormat[MetadataEnvelope]
}
