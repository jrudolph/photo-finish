package net.virtualvoid.fotofinish

import java.io.File

import akka.actor.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{ Sink, Source }
import net.virtualvoid.fotofinish.metadata.{ ExifBaseDataExtractor, IngestionDataExtractor, MetadataEntry }

import scala.concurrent.duration._

object StreamedScannerMain extends App {
  implicit val system = ActorSystem()
  import system.dispatcher

  // setup main stream
  val (killSwitch, journal) = MetadataProcess.journal(Settings.manager, Settings.metadataStore)

  val queue =
    Source.queue[MetadataEntry](1000, OverflowStrategy.dropNew)
      .via(journal)
      .to(Sink.foreach(println))
      .run()

  journal
    .join(
      MetadataProcess.asStream(new MetadataIsCurrentProcess(ExifBaseDataExtractor), Settings.manager)
        .mapAsync(4)(se => se())
        .mapConcat { entries =>
          println(s"Got ${entries.size} more entries")
          entries
        }
    )
    .run()

  system.registerOnTermination(killSwitch.shutdown())

  val dir = new File("/home/johannes/git/self/photo-finish/tmprepo/ingest")
  println(s"Ingesting new files from $dir")
  val is = new Scanner(Settings.config, Settings.manager).scan(dir)
  is
    .map(IngestionDataExtractor.extractMetadata(_).get)
    .foreach(queue.offer(_))

  system.scheduler.scheduleOnce(5.seconds) {
    killSwitch.shutdown()
    system.terminate()
  }
}
