package net.virtualvoid.fotofinish.metadata

import java.io.File

import org.apache.pekko.http.scaladsl.model.DateTime

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

trait ExtractionContext {
  implicit def executionContext: ExecutionContext
  def accessData[T](id: Id)(f: File => Future[T]): Future[T]
  def accessDataSync[T](id: Id)(f: File => T): Future[T] = accessData(id)(file => Future(f(file)))
}

trait MetadataExtractor {
  type EntryT
  def kind: String
  def version: Int
  def metadataKind: MetadataKind.Aux[EntryT]
  def dependsOn: Vector[MetadataKind]

  final def extract(id: Id, dependencies: Vector[MetadataEntry], ctx: ExtractionContext): Future[MetadataEntry.Aux[EntryT]] =
    Try(extractEntry(id, dependencies, ctx))
      .recover[Future[EntryT]] {
        case ex => Future.failed(ex)
      }
      .get // FIXME: recover + get really the best way to do this?
      .map(value =>
        MetadataEntry(
          id,
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
  def precondition(id: Id, dependencies: Vector[MetadataEntry]): Option[String] = None

  def upgradeExisting(existing: MetadataEntry.Aux[EntryT], dependencies: Vector[MetadataEntry]): MetadataExtractor.Upgrade = MetadataExtractor.Keep

  protected def extractEntry(id: Id, dependencies: Vector[MetadataEntry], ctx: ExtractionContext): Future[EntryT]
}

object MetadataExtractor {
  // FIXME: replace with more flexible builder pattern

  def apply(_kind: String, _version: Int, metadata: MetadataKind)(f: (Id, ExtractionContext) => Future[metadata.T]): MetadataExtractor =
    new MetadataExtractor {
      type EntryT = metadata.T
      def kind: String = _kind
      def version: Int = _version
      def metadataKind: MetadataKind.Aux[EntryT] = metadata
      def dependsOn: Vector[MetadataKind] = Vector.empty
      protected def extractEntry(id: Id, dependencies: Vector[MetadataEntry], ctx: ExtractionContext): Future[EntryT] =
        f(id, ctx)
    }

  def dep1(_kind: String, _version: Int, metadata: MetadataKind, dep1: MetadataKind)(f: (Id, dep1.T, ExtractionContext) => Future[metadata.T]): MetadataExtractor =
    new MetadataExtractor {
      type EntryT = metadata.T
      def kind: String = _kind
      def version: Int = _version
      def metadataKind: MetadataKind.Aux[EntryT] = metadata
      def dependsOn: Vector[MetadataKind] = Vector(dep1)
      protected def extractEntry(id: Id, dependencies: Vector[MetadataEntry], ctx: ExtractionContext): Future[EntryT] =
        f(id, dependencies(0).value.asInstanceOf[dep1.T], ctx)
    }

  def cond1(_kind: String, _version: Int, metadata: MetadataKind, cond1: MetadataKind)(p: cond1.T => Option[String])(f: (Id, ExtractionContext) => Future[metadata.T]): MetadataExtractor =
    new MetadataExtractor {
      type EntryT = metadata.T
      def kind: String = _kind
      def version: Int = _version
      def metadataKind: MetadataKind.Aux[EntryT] = metadata
      def dependsOn: Vector[MetadataKind] = Vector(cond1)
      protected def extractEntry(id: Id, dependencies: Vector[MetadataEntry], ctx: ExtractionContext): Future[EntryT] =
        f(id, ctx)

      override def precondition(id: Id, dependencies: Vector[MetadataEntry]): Option[String] =
        p(dependencies(0).value.asInstanceOf[cond1.T])
    }

  sealed trait Upgrade
  case object Keep extends Upgrade
  case object RerunExtractor extends Upgrade
  //case class PublishUpgraded(newEntry: MetadataEntry) extends Upgrade
}