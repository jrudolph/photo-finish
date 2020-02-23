package net.virtualvoid.fotofinish.apps

import akka.actor.ActorSystem
import net.virtualvoid.fotofinish.metadata.{ ExifBaseData, Orientation }
import net.virtualvoid.fotofinish.{ MetadataApp, Settings }

object FindRotatedImages extends App {
  implicit val system = ActorSystem()
  import system.dispatcher

  val app = MetadataApp(Settings.config)
  import spray.json.DefaultJsonProtocol._
  val curVal = app.aggregation("find-rotated", 1, ExifBaseData, 0L)((count, next) => count + (if (!next.orientation.contains(Orientation.Normal)) 1 else 0))
  curVal().onComplete { res =>
    println(s"The number of rotated images is ${res}")
    system.terminate()
  }
}
