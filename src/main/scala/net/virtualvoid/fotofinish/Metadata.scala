package net.virtualvoid.fotofinish

import java.io.FileInputStream
import java.io.FileOutputStream
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

import akka.http.scaladsl.model.DateTime
import com.drew.imaging.ImageMetadataReader
import com.drew.metadata.Directory
import com.drew.metadata.exif.ExifDirectoryBase
import com.drew.metadata.exif.ExifIFD0Directory
import com.drew.metadata.exif.ExifSubIFDDirectory
import spray.json._

import scala.collection.immutable
import scala.io.Source
import scala.reflect.ClassTag
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
  private implicit def entryFormat = new JsonFormat[MetadataEntry[EntryT]] {
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
  def getEntry[T: ClassTag]: Option[MetadataEntry[T]] =
    entries.collectFirst {
      case e @ MetadataEntry(_, _, data: T) => e.asInstanceOf[MetadataEntry[T]]
    }
  def get[T: ClassTag]: Option[T] = getEntry[T].map(_.data)
}

object MetadataStore {
  val RegisteredMetadataExtractors: immutable.Seq[MetadataExtractor] = Vector(
    ExifBaseDataExtractor,
    IngestionDataExtractor
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

  def load(target: FileInfo): Metadata = Metadata {
    if (!target.metadataFile.exists()) Nil
    else
      Source.fromInputStream(new GZIPInputStream(new FileInputStream(target.metadataFile))).getLines()
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
      if (!infos.exists(_.extractor == ex)) {
        println(s"Metadata [${ex.kind}] missing for [${target.repoFile}], rerunning analysis...")
        val result = ex.extractMetadata(target)
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
    originalFileName: String,
    originalFilePath: String,
    fileSize:         Long
)
object IngestionDataExtractor extends MetadataExtractor {
  type EntryT = IngestionData
  override def kind: String = "ingestion-data"
  override def version: Int = 1

  override protected def extract(file: FileInfo): IngestionData =
    IngestionData(
      file.originalFileName.getName,
      file.originalFileName.getParent,
      file.originalFileName.length()
    )
  import DefaultJsonProtocol._
  override implicit def metadataFormat: JsonFormat[IngestionData] = jsonFormat3(IngestionData)
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