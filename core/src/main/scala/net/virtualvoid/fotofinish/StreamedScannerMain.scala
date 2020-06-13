package net.virtualvoid.fotofinish

import java.io.File

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source

import scala.concurrent.duration._

object StreamedScannerMain extends App {
  implicit val system = ActorSystem()
  import system.dispatcher

  val app = MetadataApp(Settings.config)

  val dir = new File("/home/johannes/Fotos/Kameras/")
  println(s"Ingesting new files from $dir")
  val is = new Scanner(Settings.config).scan(dir)
  Source.fromIterator(() => is)
    .runWith(app.ingestionDataSink)

  /*system.scheduler.scheduleOnce(240.seconds) {
    println("Shutting down...")
    app.journal.shutdown()
    system.scheduler.scheduleOnce(1.seconds)(system.terminate())
  }*/
}
