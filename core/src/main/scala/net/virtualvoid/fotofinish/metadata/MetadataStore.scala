package net.virtualvoid.fotofinish
package metadata

import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

import scala.collection.immutable
import scala.io.Source
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import spray.json._

import scala.util.control.NonFatal

object MetadataStore {
  val RegisteredMetadataExtractors: immutable.Seq[MetadataExtractor] = Vector(
    ExifBaseDataExtractor,
    IngestionDataExtractor,
    ThumbnailExtractor,
    FaceDataExtractor
  )

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
    val extractor = findExtractor(header)
    extractor
      .flatMap[MetadataEntry[_]] { e =>
        val entry = e.get(jsonValue)

        Some(entry).filter(e.isCorrect)
      }
  }.recover {
    case e =>
      println(s"Couldn't read metadata entry [$entry] because of [${e.getMessage}]")
      None
  }.get

  def findExtractor(header: MetadataHeader): Option[MetadataExtractor] =
    RegisteredMetadataExtractors.find(e => e.kind == header.kind && e.version == header.version)
}

sealed trait MetadataStatus
object MetadataStatus {
  case object Missing extends MetadataStatus
  final case class ExistingAndCurrent(metadataEntries: immutable.Seq[MetadataEntry[_]]) extends MetadataStatus
  final case class ExistingButOutdated(currentEntries: immutable.Seq[MetadataEntry[_]]) extends MetadataStatus
}

class MetadataStore(repoConfig: RepositoryConfig) {
  import MetadataStore._

  /**
   * Reruns all known extractors when metadata is missing.
   */
  def updateMetadata(target: FileInfo): immutable.Seq[MetadataEntry[_]] = {
    val infos = load(target)
    RegisteredMetadataExtractors
      .flatMap(e => updateMetadataFor(target, e, infos).toVector)
  }

  def metadataStatus(target: FileInfo): Map[MetadataExtractor, MetadataStatus] = {
    val infos = load(target)
    RegisteredMetadataExtractors
      .map { e =>
        val es = infos.getEntries(e.classTag)
        val status =
          if (es.isEmpty) MetadataStatus.Missing
          else if (e.isCurrent(target, es)) MetadataStatus.ExistingAndCurrent(es)
          else MetadataStatus.ExistingButOutdated(es)
        e -> status
      }.toMap
  }

  def updateMetadataFor(target: FileInfo, extractor: MetadataExtractor): Option[MetadataEntry[_]] =
    updateMetadataFor(target, extractor, load(target))

  private def updateMetadataFor(target: FileInfo, extractor: MetadataExtractor, existing: Metadata): Option[MetadataEntry[_]] = {
    val exInfos = existing.getEntries(extractor.classTag)
    if (exInfos.isEmpty || !extractor.isCurrent(target, exInfos)) {
      println(s"Metadata [${extractor.kind}] missing (${exInfos.isEmpty}) or not current (${!extractor.isCurrent(target, exInfos)}) for [${target.repoFile}], rerunning analysis...")
      extractor.extractMetadata(target) match {
        case Success(entry) =>
          store(entry)
          Some(entry)
        case Failure(exception) =>
          println(s"Metadata extraction [${extractor.kind} failed for [${target.repoFile}] with ${exception.getMessage}")
          None
      }
    } else None
  }

  private def store[T](entry: MetadataEntry[T]): Unit = try {
    val baos = new ByteArrayOutputStream()
    val out = new GZIPOutputStream(baos)
    out.write(entry.extractor.create(entry).compactPrint.getBytes("utf8"))
    out.write('\n')
    out.close()
    baos.close()

    destinationsFor(entry).foreach { dest =>
      atomicallyAppendTo(dest, baos.toByteArray)
    }
  } catch {
    case NonFatal(e) =>
      e.printStackTrace()
      throw e
  }

  private def destinationsFor(entry: MetadataEntry[_]): Seq[File] =
    Seq(
      repoConfig.metadataFile(entry.header.forData),
      repoConfig.allMetadataFile,
      repoConfig.metadataCollectionFor(entry.extractor)
    )

  private def atomicallyAppendTo(target: File, data: Array[Byte]): Unit = {
    // FIXME: this is not atomic
    val fos = new FileOutputStream(target, true)
    try fos.write(data)
    finally fos.close()
  }
}