package net.virtualvoid.fotofinish

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes
import java.util.Base64
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

import akka.http.scaladsl.model.DateTime
import akka.util.ByteString
import com.drew.imaging.ImageMetadataReader
import com.drew.metadata.Directory
import com.drew.metadata.exif.ExifDirectoryBase
import com.drew.metadata.exif.ExifIFD0Directory
import com.drew.metadata.exif.ExifSubIFDDirectory
import javax.imageio.ImageIO
import spray.json._

import scala.collection.immutable
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success
import scala.util.Try

final case class MetadataHeader(
    created: DateTime,
    version: Int,
    forData: Hash,
    kind:    String
)

final case class MetadataEntry[T](
    header:    MetadataHeader,
    extractor: MetadataExtractor { type EntryT = T },
    data:      T
)

object MetadataJsonProtocol {
  def error(message: String): Nothing =
    throw new DeserializationException(message)

  import DefaultJsonProtocol._
  implicit val dateTimeFormat = new JsonFormat[DateTime] {
    override def read(json: JsValue): DateTime = json match {
      case JsString(data) => DateTime.fromIsoDateTimeString(data).getOrElse(error(s"Date could not be read [$data]"))
    }
    override def write(obj: DateTime): JsValue = JsString(obj.toIsoDateTimeString())
  }
  implicit val byteStringFormat = new JsonFormat[ByteString] {
    override def read(json: JsValue): ByteString = json match {
      case JsString(data) => ByteString.fromArray(Base64.getDecoder.decode(data))
    }
    override def write(obj: ByteString): JsValue = {
      val data = Base64.getEncoder.encodeToString(obj.toArray)
      JsString(data)
    }
  }

  implicit val hashFormat = new JsonFormat[Hash] {
    override def read(json: JsValue): Hash = json match {
      case JsString(data) => Hash.fromPrefixedString(data).getOrElse(error(s"Prefixed hash string could not be read [$data]"))
    }
    override def write(obj: Hash): JsValue = JsString(obj.toString)
  }
  implicit val metadataHeaderFormat = jsonFormat4(MetadataHeader.apply _)
}

trait MetadataExtractor { thisExtractor =>
  type EntryT
  def kind: String
  def version: Int
  implicit def classTag: ClassTag[EntryT]

  /** Override to check allow new entries created for the same version */
  def isCurrent(file: FileInfo, entries: immutable.Seq[MetadataEntry[EntryT]]): Boolean = true

  // TODO: support streaming metadata extraction?
  def extractMetadata(file: FileInfo): Option[MetadataEntry[EntryT]] =
    Try {
      MetadataEntry[EntryT](
        MetadataHeader(
          DateTime.now,
          version,
          file.hash,
          kind
        ),
        this,
        extract(file)
      )
    }.toOption

  protected def extract(file: FileInfo): EntryT

  implicit def metadataFormat: JsonFormat[EntryT]

  import JsonExtra._
  import MetadataJsonProtocol._
  private implicit val entryFormat = new JsonFormat[MetadataEntry[EntryT]] {
    override def read(json: JsValue): MetadataEntry[EntryT] = {
      val header = json.convertTo[MetadataHeader]
      require(header.kind == kind && header.version == version)
      val data = json.field("data").convertTo[EntryT]
      MetadataEntry[EntryT](header, thisExtractor, data)
    }
    override def write(obj: MetadataEntry[EntryT]): JsValue =
      obj.header.toJson + ("data" -> obj.data.toJson)
  }

  def get(jsonData: JsValue): MetadataEntry[EntryT] =
    jsonData.convertTo[MetadataEntry[EntryT]]
  def create(entry: MetadataEntry[EntryT]): JsValue =
    entry.toJson
}

final case class Metadata(entries: immutable.Seq[MetadataEntry[_]]) {
  def getEntry[E: ClassTag]: Option[MetadataEntry[E]] =
    entries.reverse.collectFirst {
      case e @ MetadataEntry(_, _, data: E) => e.asInstanceOf[MetadataEntry[E]]
    }
  def getEntries[E: ClassTag]: immutable.Seq[MetadataEntry[E]] =
    entries.collect {
      case e @ MetadataEntry(_, _, data: E) => e.asInstanceOf[MetadataEntry[E]]
    }

  def get[T: ClassTag]: Option[T] = getEntry[T].map(_.data)

  def get[T](shortcut: MetadataShortcuts.ShortCut[T]): T = shortcut(this)
}

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
    try {
      out.write(metadata.extractor.create(metadata).compactPrint.getBytes("utf8"))
      out.write('\n')
    } finally out.close()
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
    val infos = load(target).entries
    RegisteredMetadataExtractors.flatMap { ex =>
      val exInfos = infos.collect {
        case e: MetadataEntry[ex.EntryT] if e.extractor == ex => e
      }
      if (exInfos.isEmpty || !ex.isCurrent(target, exInfos)) {
        println(s"Metadata [${ex.kind}] missing or not current for [${target.repoFile}], rerunning analysis...")
        val result: Option[MetadataEntry[ex.EntryT]] =
          Try(ex.extractMetadata(target)) match {
            case Success(m) =>
              m
            case Failure(exception) =>
              println(s"Metadata extraction [${ex.kind} failed for [${target.repoFile}] with ${exception.getMessage}")
              None
          }
        if (result.isDefined) store(result.get, repoConfig)
        result.toSeq
      } else immutable.Seq.empty[MetadataEntry[_]]
    }
  }

  def findExtractor(header: MetadataHeader): Option[MetadataExtractor] =
    RegisteredMetadataExtractors.find(e => e.kind == header.kind && e.version == header.version)
}

object JsonExtra {
  implicit class RichJsValue(val jsValue: JsValue) extends AnyVal {
    def +(field: (String, JsValue)): JsObject = {
      val obj = jsValue.asJsObject
      obj.copy(fields = obj.fields + field)
    }
    def field(name: String): JsValue =
      jsValue.asJsObject.fields(name)
  }
}

final case class IngestionData(
    fileSize:                 Long,
    originalFileName:         String,
    originalFilePath:         String,
    originalFileCreationDate: DateTime,
    originalFileModifiedDate: DateTime,
    repoFileModifiedDate:     DateTime
) {
  def originalFullFilePath: String = originalFilePath + "/" + originalFileName
}
object IngestionDataExtractor extends MetadataExtractor {
  type EntryT = IngestionData
  override def kind: String = "ingestion-data"
  override def version: Int = 2
  override def classTag: ClassTag[IngestionData] = implicitly[ClassTag[IngestionData]]

  override protected def extract(file: FileInfo): IngestionData =
    IngestionData(
      file.originalFile.length(),
      file.originalFile.getName,
      file.originalFile.getParent,
      DateTime(Files.readAttributes(file.originalFile.toPath, classOf[BasicFileAttributes]).creationTime().toMillis),
      DateTime(file.originalFile.lastModified()),
      DateTime(file.repoFile.lastModified())
    )
  import DefaultJsonProtocol._
  import MetadataJsonProtocol.dateTimeFormat
  override implicit val metadataFormat: JsonFormat[IngestionData] = jsonFormat6(IngestionData)

  override def isCurrent(file: FileInfo, entries: immutable.Seq[MetadataEntry[IngestionData]]): Boolean =
    entries.exists(_.data.originalFileName == file.originalFile.getName)
}

final case class ExifBaseData(
    width:       Option[Int],
    height:      Option[Int],
    dateTaken:   Option[DateTime],
    cameraModel: Option[String]
)
object ExifBaseDataExtractor extends MetadataExtractor {
  override type EntryT = ExifBaseData

  override def kind: String = "exif-base-data"
  override def version: Int = 1

  override def classTag: ClassTag[ExifBaseData] = implicitly[ClassTag[ExifBaseData]]

  import DefaultJsonProtocol._
  import MetadataJsonProtocol.dateTimeFormat
  override implicit def metadataFormat: JsonFormat[ExifBaseData] = jsonFormat4(ExifBaseData)

  override protected def extract(file: FileInfo): ExifBaseData = {
    val metadata = ImageMetadataReader.readMetadata(file.repoFile)
    val dir0 = Option(metadata.getFirstDirectoryOfType(classOf[ExifIFD0Directory]))
    val dir1 = Option(metadata.getFirstDirectoryOfType(classOf[ExifSubIFDDirectory]))
    val dirs = dir0.toSeq ++ dir1.toSeq

    def entry[T](tped: (Directory, Int) => T): Int => Option[T] =
      tag =>
        dirs.collectFirst {
          case dir if dir.containsTag(tag) => Option(tped(dir, tag))
        }.flatten
    def dateEntry = entry((dir, tag) => DateTime(dir.getDate(tag).getTime))
    def intEntry = entry(_.getInt(_))
    def stringEntry = entry(_.getString(_))

    val width = intEntry(ExifDirectoryBase.TAG_EXIF_IMAGE_WIDTH)
    val height = intEntry(ExifDirectoryBase.TAG_EXIF_IMAGE_HEIGHT)
    val date = dateEntry(ExifDirectoryBase.TAG_DATETIME_ORIGINAL)
    val model = stringEntry(ExifDirectoryBase.TAG_MODEL)

    ExifBaseData(width, height, date, model)
  }
}

final case class Thumbnail(
    width:  Int,
    height: Int,
    data:   ByteString
)
object ThumbnailExtractor extends MetadataExtractor {
  type EntryT = Thumbnail

  override def kind: String = "thumbnail"
  override def version: Int = 2

  override def classTag: ClassTag[Thumbnail] = implicitly[ClassTag[Thumbnail]]

  override protected def extract(file: FileInfo): Thumbnail = {
    import sys.process._
    val baos = new ByteArrayOutputStream
    val res = s"""convert ${file.repoFile.getCanonicalPath} -thumbnail 150 -quality 20 -auto-orient -""".#>(baos).!
    require(res == 0)
    val imageBytes = ByteString.fromArray(baos.toByteArray)
    // TODO: optimize memory usage
    val image = ImageIO.read(new ByteArrayInputStream(baos.toByteArray))

    Thumbnail(image.getWidth, image.getHeight, imageBytes)
  }

  import DefaultJsonProtocol._
  import MetadataJsonProtocol.byteStringFormat
  override implicit val metadataFormat: JsonFormat[Thumbnail] = jsonFormat3(Thumbnail)
}

object MetadataShortcuts {
  type ShortCut[T] = Metadata => T

  def optional[E: ClassTag, T](f: E => Option[T]): ShortCut[Option[T]] = _.getEntry[E].flatMap(e => f(e.data))
  def manyFromSingle[E: ClassTag, T](f: E => T): ShortCut[immutable.Seq[T]] = _.getEntries[E].map(e => f(e.data))

  val DateTaken = optional[ExifBaseData, DateTime](_.dateTaken)
  val CameraModel = optional[ExifBaseData, String](_.cameraModel)
  val OriginalFileNames = manyFromSingle[IngestionData, String](_.originalFileName)
  val OriginalFullFilePaths = manyFromSingle[IngestionData, String](x => x.originalFullFilePath)
}