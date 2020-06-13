package net.virtualvoid.fotofinish.process

import java.io.{ File, FileOutputStream }
import java.nio.file.{ Files, StandardCopyOption }
import java.util.zip.GZIPOutputStream

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Compression, FileIO, Framing, Sink }
import akka.util.ByteString
import net.virtualvoid.fotofinish.metadata.MetadataEntry
import spray.json.JsonFormat

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.util.control.NonFatal

trait LineBasedJsonSnaphotProcess extends MetadataProcess {
  import spray.json.DefaultJsonProtocol._
  import spray.json._

  private case class SnapshotHeader(processId: String, processVersion: Int, currentSeqNr: Long)
  private implicit val headerFormat = jsonFormat3(SnapshotHeader.apply)
  def saveSnapshot(targetFile: File, config: ProcessConfig, snapshot: Snapshot[S]): S = {
    val tmpFile = File.createTempFile(targetFile.getName, ".tmp", targetFile.getParentFile)
    val os = new GZIPOutputStream(new FileOutputStream(tmpFile))

    try {
      implicit val seFormat = stateEntryFormat(config.entryFormat)
      os.write(SnapshotHeader(snapshot.processId, snapshot.processVersion, snapshot.currentSeqNr).toJson.compactPrint.getBytes("utf8"))
      os.write('\n')
      stateAsEntries(snapshot.state).foreach { entry =>
        os.write(entry.toJson.compactPrint.getBytes("utf8"))
        os.write('\n')
      }
    } finally os.close()
    Files.move(tmpFile.toPath, targetFile.toPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
    snapshot.state
  }
  def loadSnapshot(file: File, config: ProcessConfig)(implicit system: ActorSystem): Option[Snapshot[S]] = {
    import system.dispatcher

    if (file.exists) {
      implicit val seFormat: JsonFormat[StateEntryT] = stateEntryFormat(config.entryFormat)

      val headerAndEntriesF =
        FileIO.fromPath(file.toPath)
          .via(Compression.gunzip())
          .via(Framing.delimiter(ByteString("\n"), 1000000000))
          .map(_.utf8String)
          .prefixAndTail(1)
          .runWith(Sink.head)
          .flatMap {
            case (Seq(header), entries) =>
              val h = header.parseJson.convertTo[SnapshotHeader]
              entries
                .mapAsyncUnordered(8)(str => Future(str.parseJson.convertTo[StateEntryT]))
                .runWith(Sink.seq)
                .map(es =>
                  Snapshot[S](h.processId, h.processVersion, h.currentSeqNr, entriesAsState(es))
                )
          }

      // FIXME
      val hs = Await.ready(headerAndEntriesF, 60.seconds)

      hs.value.get
        .map { s =>
          require(s.processId == id, s"Unexpected process ID in snapshot [${s.processId}]. Expected [$id].")
          require(s.processVersion == version, s"Wrong version in snapshot [${s.processVersion}]. Expected [$version].")

          Some(s.copy(state = initializeStateSnapshot(s.state)))
        }
        .recover {
          case NonFatal(ex) =>
            println(s"Reading snapshot failed because of ${ex.getMessage}. Discarding snapshot.")
            ex.printStackTrace()
            None
        }
        .get
    } else
      None
  }

  /** Allows to prepare state loaded from snapshot */
  protected def initializeStateSnapshot(state: S): S = state

  protected type StateEntryT
  protected def stateAsEntries(state: S): Iterator[StateEntryT]
  protected def entriesAsState(entries: Iterable[StateEntryT]): S
  protected def stateEntryFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[StateEntryT]
}
trait SingleEntryState extends LineBasedJsonSnaphotProcess {
  override type StateEntryT = S

  protected def stateAsEntries(state: S): Iterator[S] = Iterator(state)
  protected def entriesAsState(entries: Iterable[S]): S = entries.head
  protected def stateEntryFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[S] = stateFormat
  protected def stateFormat(implicit entryFormat: JsonFormat[MetadataEntry]): JsonFormat[S]
}