package net.virtualvoid.fotofinish.util

import net.virtualvoid.fotofinish.metadata.MetadataEntry

import scala.concurrent.Future

trait PersistentStream[+T] {
  def process[U](offsetStore: OffsetStore, flow: PersistentFlow[T, U]): PersistentStream[U] = ???
}

trait OffsetStore {
  def lastProcessed: Long
  def commit(seq: Long): Unit
}

trait PersistentFlow[-T, +U] {
  def map[V](f: U => V): PersistentFlow[T, V]
  def mapAsync[V](parallelism: Int)(f: U => Future[V]): PersistentFlow[T, V]
  def filter(p: U => Boolean): PersistentFlow[T, U]
}

object PersistentStreams {
  def AllMetadata: PersistentStream[MetadataEntry[_]] = ???

}

object TheApp {
  // Ingestion stream:
  //   * traverse ingestion directories
  //   * find new files and add to repo
  //   * post IngestionData

  // All Metadata:
  //   * merge data from all metadata generation streams?

  // ExifBaseData:
  //   *
}