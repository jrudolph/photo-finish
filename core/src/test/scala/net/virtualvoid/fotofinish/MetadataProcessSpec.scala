package net.virtualvoid.fotofinish

import org.scalatest.freespec.AnyFreeSpec

class MetadataProcessSpec extends AnyFreeSpec {
  "The MetadataProcess should" - {
    "provide events from journal" - {
      "persisted events" in pending
      "persisted events and then live events" in pending
    }
    "skip duplicate sequence numbers when live events start" in pending
    "error when there's a gap between persisted events and live events" in pending
    "warn (error?) on duplicate sequence numbers otherwise" in pending
    "snapshot process on exit and reload snapshot on restart" in pending
    "allow pushing IngestionData" in pending
    "support conditional execution of extractors" in pending
  }
}
