package net.virtualvoid.fotofinish

import java.io.File

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, MergeHub, Sink}
import net.virtualvoid.fotofinish.MetadataProcess.SideEffect
import net.virtualvoid.fotofinish.metadata.ExifBaseDataExtractor

import scala.concurrent.duration._

object StreamedScannerMain extends App {
  val parallelism = 8

  implicit val system = ActorSystem()
  import system.dispatcher

  // setup main stream
  val (killSwitch, journalIn, journalOut) = MetadataProcess.journal(Settings.manager, Settings.metadataStore)

  /*  val queue =
    Source.queue[MetadataEntry](1000, OverflowStrategy.dropNew)
      .via(journal)
      .to(Sink.foreach(println))
      .run()*/

  val executor: Sink[SideEffect, Any] =
    MergeHub.source[SideEffect]
      .mapAsyncUnordered(parallelism)(_())
      .mapConcat(identity)
      .to(journalIn)
      .run()

  def runProcess(process: MetadataProcess): process.Api =
    journalOut
      .viaMat(MetadataProcess.asStream(process, Settings.manager))(Keep.right)
      .to(executor)
      .run()

  runProcess(new MetadataIsCurrentProcess(ExifBaseDataExtractor))
  val ingestor = runProcess(new IngestionController)

  system.registerOnTermination(killSwitch.shutdown())

  val dir = new File("/home/johannes/git/self/photo-finish/tmprepo/ingest")
  println(s"Ingesting new files from $dir")
  val is = new Scanner(Settings.config, Settings.manager).scan(dir)
  is.foreach(ingestor)

  system.scheduler.scheduleOnce(5.seconds) {
    println("Shutting down...")
    killSwitch.shutdown()
    system.terminate()
  }
}
