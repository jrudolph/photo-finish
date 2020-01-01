package net.virtualvoid.fotofinish

import java.io.File

import akka.actor.ActorSystem

import scala.concurrent.duration._

object StreamedScannerMain extends App {
  implicit val system = ActorSystem()
  import system.dispatcher

  val app = MetadataApp(Settings.config)

  //journal.source(0).runForeach(println)
  val dir = new File("/home/johannes/git/self/photo-finish/tmprepo/ingest")
  println(s"Ingesting new files from $dir")
  val is = new Scanner(Settings.config, Settings.manager).scan(dir)
  is.foreach(app.ingest)

  system.scheduler.scheduleOnce(5.seconds) {
    println("Shutting down...")
    app.journal.shutdown()
    system.scheduler.scheduleOnce(1.seconds)(system.terminate())
  }
}
