package net.virtualvoid.fotofinish.metadata

import java.io.File

import akka.http.scaladsl.model.DateTime
import net.virtualvoid.fotofinish.Hash
import net.virtualvoid.fotofinish.metadata.Id.Hashed

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

trait ExtractionContext {
  implicit def executionContext: ExecutionContext
  def accessData[T](hash: Hash)(f: File => Future[T]): Future[T]
  def accessDataSync[T](hash: Hash)(f: File => T): Future[T] = accessData(hash)(file => Future(f(file)))
}

trait MetadataExtractor {
  type EntryT
  def kind: String
  def version: Int
  def metadataKind: MetadataKind.Aux[EntryT]
  def dependsOn: Vector[MetadataKind]

  final def extract(hash: Hash, dependencies: Vector[MetadataEntry], ctx: ExtractionContext): Future[MetadataEntry.Aux[EntryT]] =
    Try(extractEntry(hash, dependencies, ctx))
      .recover[Future[EntryT]] {
        case ex => Future.failed(ex)
      }
      .get // FIXME: recover + get really the best way to do this?
      .map(value =>
        MetadataEntry(
          Hashed(hash),
          Vector.empty,
          metadataKind,
          CreationInfo(DateTime.now, inferred = true, Creator.fromExtractor(this)),
          value))(ctx.executionContext)

  /**
   * Allows to specify a precondition to run against the dependency values that is run before extract is called.
   *
   * Return None if precondition is met or Some(cause) if there's an obstacle.
   *
   * FIXME: is there a better type or name for that method?
   */
  def precondition(hash: Hash, dependencies: Vector[MetadataEntry]): Option[String] = None

  def upgradeExisting(existing: MetadataEntry.Aux[EntryT], dependencies: Vector[MetadataEntry]): MetadataExtractor.Upgrade = MetadataExtractor.Keep

  protected def extractEntry(hash: Hash, dependencies: Vector[MetadataEntry], ctx: ExtractionContext): Future[EntryT]
}

object MetadataExtractor {
  // FIXME: replace with more flexible builder pattern

  def apply(_kind: String, _version: Int, metadata: MetadataKind)(f: (Hash, ExtractionContext) => Future[metadata.T]): MetadataExtractor =
    new MetadataExtractor {
      type EntryT = metadata.T
      def kind: String = _kind
      def version: Int = _version
      def metadataKind: MetadataKind.Aux[EntryT] = metadata
      def dependsOn: Vector[MetadataKind] = Vector.empty
      protected def extractEntry(hash: Hash, dependencies: Vector[MetadataEntry], ctx: ExtractionContext): Future[EntryT] =
        f(hash, ctx)
    }

  def dep1(_kind: String, _version: Int, metadata: MetadataKind, dep1: MetadataKind)(f: (Hash, dep1.T, ExtractionContext) => Future[metadata.T]): MetadataExtractor =
    new MetadataExtractor {
      type EntryT = metadata.T
      def kind: String = _kind
      def version: Int = _version
      def metadataKind: MetadataKind.Aux[EntryT] = metadata
      def dependsOn: Vector[MetadataKind] = Vector(dep1)
      protected def extractEntry(hash: Hash, dependencies: Vector[MetadataEntry], ctx: ExtractionContext): Future[EntryT] =
        f(hash, dependencies(0).value.asInstanceOf[dep1.T], ctx)
    }

  def cond1(_kind: String, _version: Int, metadata: MetadataKind, cond1: MetadataKind)(p: cond1.T => Option[String])(f: (Hash, ExtractionContext) => Future[metadata.T]): MetadataExtractor =
    new MetadataExtractor {
      type EntryT = metadata.T
      def kind: String = _kind
      def version: Int = _version
      def metadataKind: MetadataKind.Aux[EntryT] = metadata
      def dependsOn: Vector[MetadataKind] = Vector(cond1)
      protected def extractEntry(hash: Hash, dependencies: Vector[MetadataEntry], ctx: ExtractionContext): Future[EntryT] =
        f(hash, ctx)

      override def precondition(hash: Hash, dependencies: Vector[MetadataEntry]): Option[String] =
        p(dependencies(0).value.asInstanceOf[cond1.T])
    }

  sealed trait Upgrade
  case object Keep extends Upgrade
  case object RerunExtractor extends Upgrade
  //case class PublishUpgraded(newEntry: MetadataEntry) extends Upgrade
}