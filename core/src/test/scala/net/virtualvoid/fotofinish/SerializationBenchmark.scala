package net.virtualvoid.fotofinish

import akka.actor.ActorSystem

object SerializationBenchmark extends App {
  implicit val system = ActorSystem()
  (0 to 100).foreach(_ => MetadataProcess.deserializeState(PerObjectMetadataCollector, Settings.config))
}
