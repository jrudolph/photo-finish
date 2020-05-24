package net.virtualvoid.fotofinish

import java.io.File


import scala.concurrent.duration._
import net.virtualvoid.fotofinish.metadata._

object Settings {
  /*val repo = new File("/home/johannes/Fotos/tmp/repo")
  val meta = new File("/home/johannes/Fotos/tmp/repo/metadata")
  val linkDir = new File("/home/johannes/FotosSorted")*/
  val repo = new File("/home/johannes/git/self/photo-finish/tmprepo/objects")
  val meta = new File("/home/johannes/git/self/photo-finish/tmprepo/metadata")
  val linkDir = new File("/home/johannes/git/self/photo-finish/tmprepo/links")
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
    //ThumbnailExtractor.instance, thumbnails are now cached and not metadata
    HashDataExtractor.instance,
    FFProbeDataExtractor.instance,
  //FaceDataExtractor.instance,
  )

  def removeLongHashEntries(entry: MetadataEntry): MetadataEntry =
    entry.target match {
      case Hashed(Hash(alg, _)) if alg != HashAlgorithm.Sha512T160 =>
        MetadataEntry(
          entry.target,
          Vector.empty,
          DeletedMetadata,
          CreationInfo(
            DateTime.now,
            inferred = true,
            Deleted
          ),
          DeletedMetadata(s"By now unsupported hash algorithm ${alg.name}", SimpleKind(entry.kind.kind, entry.kind.version), filteredOut = true)
        )
      case _ => entry
    }

  val config =
    RepositoryConfig(
      repo,
      meta,
      linkDir,
      cacheDir,
      HashAlgorithm.Sha512T160,
      knownMetadataKinds,
      removeLongHashEntries,
      autoExtractors,
      8,
      120.seconds,
      0
    )

  val scanner = new Scanner(config)
}
