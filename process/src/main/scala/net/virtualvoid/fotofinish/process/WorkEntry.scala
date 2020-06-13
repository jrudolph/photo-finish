package net.virtualvoid.fotofinish.process

import scala.language.implicitConversions

import net.virtualvoid.fotofinish.metadata.MetadataEntry

import scala.concurrent.Future

/**
 * A work entry is a job that creates metadata entries later on.
 *
 * Right now the only way to define a WorkEntry is an opaque function. Later we might introduce
 * subclasses to simplify distribution of work. Also an entry might then have metadata attached
 * to describe on which object it works and what the resource requirements are to optimize
 * scheduling of work entries.
 */
sealed trait WorkEntry {
  def run(): Future[Vector[MetadataEntry]]
}
object WorkEntry {
  implicit def opaque(f: () => Future[Vector[MetadataEntry]]): WorkEntry = Opaque(f)
  final case class Opaque(f: () => Future[Vector[MetadataEntry]]) extends WorkEntry {
    def run(): Future[Vector[MetadataEntry]] = f()
  }
}