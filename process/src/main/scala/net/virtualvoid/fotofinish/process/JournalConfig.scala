package net.virtualvoid.fotofinish.process

import net.virtualvoid.fotofinish.metadata.{ MetadataEntry, MetadataEnvelope }
import spray.json.JsonFormat

import java.io.File

trait JournalConfig {
  def metadataDir: File
  def allMetadataFile: File
  def metadataIndexFile: File
  def metadataMapper: MetadataEntry => MetadataEntry
  implicit def entryFormat: JsonFormat[MetadataEntry]
  implicit def envelopeFormat: JsonFormat[MetadataEnvelope]
}