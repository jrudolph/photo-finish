package net.virtualvoid.fotofinish.metadata

import akka.http.scaladsl.model.DateTime
import akka.util.ByteString

import scala.collection.immutable
import scala.reflect.ClassTag

object MetadataShortcuts {
  type ShortCut[T] = Metadata => T

  def optional[E: ClassTag, T](f: E => Option[T]): ShortCut[Option[T]] = _.getEntry[E].flatMap(e => f(e.data))
  def manyFromManyEntries[E: ClassTag, T](f: E => T): ShortCut[immutable.Seq[T]] = _.getEntries[E].map(e => f(e.data))
  def manyFromSingle[E: ClassTag, T](f: E => immutable.Seq[T]): ShortCut[immutable.Seq[T]] = _.get[E].toVector.flatMap(f)

  val Width = optional[ExifBaseData, Int](_.width)
  val Height = optional[ExifBaseData, Int](_.height)
  val DateTaken = optional[ExifBaseData, DateTime](_.dateTaken)
  val CameraModel = optional[ExifBaseData, String](_.cameraModel)
  val OriginalFileNames = manyFromManyEntries[IngestionData, String](_.originalFileName)
  val OriginalFolders = manyFromManyEntries[IngestionData, String](x => x.originalFilePath)
  val OriginalFullFilePaths = manyFromManyEntries[IngestionData, String](x => x.originalFullFilePath)
  val Thumbnail = optional[Thumbnail, ByteString](t => Some(t.data))
  val Faces = manyFromSingle[FaceData, FaceInfo](_.faces)
}