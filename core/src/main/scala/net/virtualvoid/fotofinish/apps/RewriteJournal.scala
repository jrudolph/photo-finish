package net.virtualvoid.fotofinish.apps

import java.io.File

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import net.virtualvoid.fotofinish.process.MetadataJournal
import net.virtualvoid.fotofinish.process.MetadataProcess.Metadata
import net.virtualvoid.fotofinish.Settings
import net.virtualvoid.fotofinish.metadata.DeletedMetadata

object RewriteJournal extends App {
  implicit val system = ActorSystem()
  import system.dispatcher

  val oldJournal = MetadataJournal(Settings.config)
  val newJournal = MetadataJournal(
    Settings.config
      .copy(
        metadataDir = new File("tmprepo3"),
        cacheDir = new File("tmprepo3/cache")
      )
  )
  newJournal.source(0).runWith(Sink.foreach {
    case Metadata(env) => if (env.seqNr % 10000 == 0) println(s"At ${env.seqNr}")
    case _             =>
  }).onComplete(_ => system.terminate())
  oldJournal.source(0L)
    .collect {
      case Metadata(env) if env.entry.kind != DeletedMetadata => env.entry
    }
    .runWith(newJournal.newEntrySink)
}
