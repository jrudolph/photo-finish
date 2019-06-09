package net.virtualvoid.metadata

import scala.reflect.ClassTag

trait MetadataProcess[T] {
  def dependencies: Seq[MetadataKindDescriptor]
}

object MetadataProcess {
  trait Builder[T] {
    def onlyIf[U](by: MetadataKindDescriptor.Aux[U])(condition: U => Boolean): Builder[T]
    def as(f: () => T): MetadataProcess[T]
  }

  def define[T](desc: MetadataKindDescriptor.Aux[T]): Builder[T] = ???
}

trait MetadataKindDescriptor {
  type Target
  def classTag: ClassTag[Target]
  def kind: String
  def version: Int
}
object MetadataKindDescriptor {
  type Aux[T] = MetadataKindDescriptor { type Target = T }

  def apply[T](kind: String, version: Int)(implicit classTag: ClassTag[T]): MetadataKindDescriptor.Aux[T] =
    MetadataKindDescriptorImpl(kind, version, classTag)

  private case class MetadataKindDescriptorImpl[T](
      kind:     String,
      version:  Int,
      classTag: ClassTag[T]
  ) extends MetadataKindDescriptor {
    override type Target = T
  }
}

object Test {
  case class MimetypeMetadata(
      detectedMimeType: Option[String]
  )

  case class ExifBaseData()

  object Metadatas {
    val Mimetype =
      MetadataKindDescriptor[MimetypeMetadata]("net.virtualvoid.mimetype", 1)
    val ExifBase =
      MetadataKindDescriptor[ExifBaseData]("net.virtualvoid.exifbase", 1)
  }

  MetadataProcess
    .define(Metadatas.ExifBase)
    .onlyIf(Metadatas.Mimetype)(_.detectedMimeType.exists(_ == "img/jpeg"))
    .as { () =>
      ExifBaseData()
    }
}