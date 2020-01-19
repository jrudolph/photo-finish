package net.virtualvoid.fotofinish

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.model.DateTime
import akka.stream.scaladsl.{ Compression, FileIO, Framing }
import akka.util.ByteString
import net.virtualvoid.fotofinish.metadata.Id.Hashed
import net.virtualvoid.fotofinish.metadata._
import spray.json.JsValue

object MetadataConverter extends App {
  implicit val system = ActorSystem()
  import system.dispatcher

  val app =
    MetadataApp(
      Settings.config
        .copy(autoExtractors = Set.empty)
    )

  case class Entry(
      kind:    String,
      version: Int,
      created: DateTime,
      forData: Hash,
      data:    JsValue
  )

  import spray.json._
  import DefaultJsonProtocol._
  import net.virtualvoid.fotofinish.metadata.MetadataJsonProtocol._
  implicit def entryFormat = jsonFormat5(Entry.apply)

  def convertEntry(entry: Entry): MetadataEntry = {
    def kindAndCreator(kind: String, version: Int): (MetadataKind, Creator) = kind match {
      case "ingestion-data"                      => (IngestionData, Ingestion)
      case "exif-base-data"                      => (ExifBaseData, Creator.fromExtractor(ExifBaseDataExtractor.instance))
      case "thumbnail"                           => (Thumbnail, Creator.fromExtractor(ThumbnailExtractor.instance))
      case "net.virtualvoid.fotofinish.FaceData" => (FaceData, Creator.fromExtractor(FaceDataExtractor.instance))
    }
    val (kind, creator) = kindAndCreator(entry.kind, entry.version)
    val inferred = kind != IngestionData
    val value: kind.T = entry.data.convertTo(kind.jsonFormat)
    val newHash = entry.forData match {
      case Hash(HashAlgorithm.Sha512, data) => Hash(HashAlgorithm.Sha512T160, data.take(HashAlgorithm.Sha512T160.byteLength))
      case x                                => throw new IllegalStateException(x.toString)
    }

    MetadataEntry[kind.T](
      Hashed(newHash),
      Vector.empty,
      kind,
      CreationInfo(
        entry.created,
        inferred,
        creator
      ),
      value
    )
  }

  case class State(
      perKindStats: Map[MetadataKind, Long]
  ) {
    def handle(entry: MetadataEntry): State =
      State(perKindStats.updated(entry.kind, perKindStats(entry.kind) + 1))
  }

  val file = new File("/mnt/hd/fotos/tmp/repo/metadata/metadata.json.gz")
  FileIO.fromPath(file.toPath)
    .via(Compression.gunzip())
    .via(Framing.delimiter(ByteString("\n"), 1000000))
    .map(_.utf8String)
    .map(_.parseJson.convertTo[Entry])
    .map(convertEntry)
    .to(app.journal.newEntrySink)
    .run()
    .onComplete { res =>
      println(s"Got result $res")
      app.journal.shutdown()
      Thread.sleep(2000)
      system.terminate()
    }
}
