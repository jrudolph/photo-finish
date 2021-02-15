package net.virtualvoid.fotofinish

import java.io.File
import akka.http.scaladsl.model.DateTime
import com.typesafe.config.ConfigFactory
import net.virtualvoid.fotofinish.metadata.Id.Hashed
import net.virtualvoid.fotofinish.metadata.MetadataJsonProtocol.SimpleKind

import scala.concurrent.duration._
import net.virtualvoid.fotofinish.metadata._

object Settings {

  val repo = new File("/home/johannes/Fotos/tmp/repo")
  val meta = new File("/home/johannes/git/self/photo-finish/tmprepo2/")
  val linkDir = new File("/home/johannes/FotosSorted")

  /*val repo = new File("/home/johannes/git/self/photo-finish/tmprepo/objects")
  val meta = new File("/home/johannes/git/self/photo-finish/tmprepo/metadata")
  val linkDir = new File("/home/johannes/git/self/photo-finish/tmprepo/links")*/

  val cacheDir = new File(meta, "cache")

  repo.mkdirs()
  meta.mkdirs()
  cacheDir.mkdirs()

  val knownMetadataKinds = Set[MetadataKind](
    IngestionData,
    FileTypeData,
    ExifBaseData,
    Thumbnail,
    FaceData,
    HashData,
    FFProbeData,
    DeletedMetadata,
  )

  val autoExtractors: Set[MetadataExtractor] = Set(
    FileTypeDataExtractor.instance,
    ExifBaseDataExtractor.instance,
    HashDataExtractor.instance,
    FFProbeDataExtractor.instance,
  //FaceDataExtractor.instance,
  )

  def removeLongHashEntries(entry: MetadataEntry): MetadataEntry =
    entry.target match {
      case Hashed(Hash(alg, _)) if alg != HashAlgorithm.Sha512T160 =>
        deleted(entry, s"By now unsupported hash algorithm ${alg.name}")
      case _ => entry
    }
  def removeThumbnails(entry: MetadataEntry): MetadataEntry =
    if (entry.kind == Thumbnail) deleted(entry, "Thumbnail metadata not supported any more")
    else entry

  def deleted(original: MetadataEntry, reason: String): MetadataEntry =
    MetadataEntry(
      original.target,
      Vector.empty,
      DeletedMetadata,
      CreationInfo(
        DateTime.now,
        inferred = true,
        Deleted
      ),
      DeletedMetadata(reason, SimpleKind(original.kind.kind, original.kind.version), filteredOut = true)
    )

  val fixedPoolSize = ConfigFactory.defaultApplication().getInt("extraction-dispatcher.thread-pool-executor.fixed-pool-size")

  val config =
    RepositoryConfig(
      repo,
      meta,
      linkDir,
      cacheDir,
      HashAlgorithm.Sha512T160,
      knownMetadataKinds,
      (removeLongHashEntries _).andThen(removeThumbnails),
      autoExtractors,
      fixedPoolSize,
      120.seconds,
      0
    )

  val scanner = new Scanner(config)
}
