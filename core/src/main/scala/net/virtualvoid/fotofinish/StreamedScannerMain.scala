package net.virtualvoid.fotofinish

import java.io.File

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source

object StreamedScannerMain extends App {
  implicit val system = ActorSystem()

  val app = MetadataApp(Settings.config.copy(autoExtractors = Set.empty)) // disable extraction when ingesting

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
