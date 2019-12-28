package net.virtualvoid.fotofinish

import java.io.File

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ MergeHub, Sink, Source }
import net.virtualvoid.fotofinish.MetadataProcess.SideEffect
import net.virtualvoid.fotofinish.metadata.{ ExifBaseDataExtractor2, FaceDataExtractor, MetadataExtractor2, ThumbnailExtractor }
//import net.virtualvoid.fotofinish.metadata.{ ExifBaseDataExtractor, FaceDataExtractor, MetadataExtractor, ThumbnailExtractor }

import scala.concurrent.duration._

object StreamedScannerMain extends App {
  val parallelism = 8
  val autoExtractors: Vector[MetadataExtractor2] = Vector(
    ExifBaseDataExtractor2,
    ThumbnailExtractor,
    FaceDataExtractor,
  )

  implicit val system = ActorSystem()
  import system.dispatcher

  // setup main stream
  val journal = MetadataProcess.journal(Settings.manager, Settings.metadataStore)

  /* val queue =
    Source.queue[MetadataEntry](1000, OverflowStrategy.dropNew)
      .via(journal)
      .to(Sink.foreach(println))
      .run()*/

  val executor: Sink[SideEffect, Any] =
    MergeHub.source[SideEffect]
      .mapAsyncUnordered(parallelism)(_())
      .mapConcat(identity)
      .to(journal.newEntrySink)
      .run()

  def runProcess(process: MetadataProcess): process.Api =
    MetadataProcess.asSource(process, Settings.manager, journal)
      .recoverWithRetries(1, {
        case ex =>
          println(s"Process [${process.id}] failed with [$ex]")
          ex.printStackTrace
          Source.empty
      })
      .to(executor)
      .run()

  val ingestor = runProcess(new IngestionController)
  autoExtractors.foreach(e => runProcess(new MetadataIsCurrentProcess(e)))

  system.registerOnTermination(journal.shutdown())

  //journal.source(0).runForeach(println)
  val dir = new File("/home/johannes/git/self/photo-finish/tmprepo/ingest")
  println(s"Ingesting new files from $dir")
  val is = new Scanner(Settings.config, Settings.manager).scan(dir)
  is.foreach(ingestor)

  system.scheduler.scheduleOnce(5.seconds) {
    println("Shutting down...")
    journal.shutdown()
    system.scheduler.scheduleOnce(1.seconds)(system.terminate())
  }
}
