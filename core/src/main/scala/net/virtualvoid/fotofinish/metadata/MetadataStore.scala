package net.virtualvoid.fotofinish
package metadata

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.OutputStream
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

import scala.collection.immutable
import scala.io.Source
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import spray.json._

object MetadataStore {
  val RegisteredMetadataExtractors: immutable.Seq[MetadataExtractor] = Vector(
    ExifBaseDataExtractor,
    IngestionDataExtractor,
    ThumbnailExtractor,
    FaceDataExtractor
  )

  def store[T](metadata: MetadataEntry[T], repoConfig: RepositoryConfig): Unit = {
    /*
     - Locate file
     - If exists: append
     - If not: create
     */
    val fos = new FileOutputStream(repoConfig.metadataFile(metadata.header.forData), true)
    val out = new GZIPOutputStream(fos)
    try storeTo(metadata, fos)
    finally out.close()
  }

  def storeTo[T](metadata: MetadataEntry[T], out: OutputStream): Unit = {
    out.write(metadata.extractor.create(metadata).compactPrint.getBytes("utf8"))
    out.write('\n')
  }

  def load(target: FileInfo): Metadata = loadAllEntriesFrom(target.metadataFile)

  def loadAllEntriesFrom(metadataFile: File): Metadata =
    Metadata {
      if (!metadataFile.exists()) Nil
      else
        Source.fromInputStream(new GZIPInputStream(new FileInputStream(metadataFile))).getLines()
          .flatMap(readMetadataEntry).toVector
    }

  def readMetadataEntry(entry: String): Option[MetadataEntry[_]] = Try {
    import MetadataJsonProtocol._
    val jsonValue = entry.parseJson
    val header = jsonValue.convertTo[MetadataHeader]
    findExtractor(header).map(_.get(jsonValue))
  }.recover {
    case e =>
      println(s"Couldn't read metadata entry [$entry] because of [${e.getMessage}]")
      None
  }.get

  /**
   * Reruns all known extractors when metadata is missing.
   */
  def updateMetadata(target: FileInfo, repoConfig: RepositoryConfig): immutable.Seq[MetadataEntry[_]] = {
    val infos = load(target)
    RegisteredMetadataExtractors
      .flatMap(e => updateMetadataFor(target, e, infos, repoConfig).toVector)
  }

  def updateMetadataFor(target: FileInfo, extractor: MetadataExtractor, repoConfig: RepositoryConfig): Option[MetadataEntry[_]] =
    updateMetadataFor(target, extractor, load(target), repoConfig)

  private def updateMetadataFor(target: FileInfo, extractor: MetadataExtractor, existing: Metadata, repoConfig: RepositoryConfig): Option[MetadataEntry[_]] = {
    val exInfos = existing.getEntries(extractor.classTag)
    if (exInfos.isEmpty || !extractor.isCurrent(target, exInfos)) {
      println(s"Metadata [${extractor.kind}] missing or not current for [${target.repoFile}], rerunning analysis...")
      val result: Option[MetadataEntry[extractor.EntryT]] =
        Try(extractor.extractMetadata(target)) match {
          case Success(m) =>
            m
          case Failure(exception) =>
            println(s"Metadata extraction [${extractor.kind} failed for [${target.repoFile}] with ${exception.getMessage}")
            None
        }
      if (result.isDefined) store(result.get, repoConfig)
      result
    } else None
  }

  def findExtractor(header: MetadataHeader): Option[MetadataExtractor] =
    RegisteredMetadataExtractors.find(e => e.kind == header.kind && e.version == header.version)
}
