package net.virtualvoid.fotofinish.process

import java.io.{ File, FileOutputStream }
import java.util.zip.{ GZIPOutputStream, ZipException }

import akka.actor.ActorSystem
import akka.pattern.after
import akka.stream.{ Attributes, FlowShape, Inlet, KillSwitches, Outlet }
import akka.stream.scaladsl.{ BroadcastHub, Compression, FileIO, Flow, Framing, Keep, MergeHub, Sink, Source }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.util.ByteString
import net.virtualvoid.fotofinish.RepositoryConfig
import net.virtualvoid.fotofinish.metadata.{ MetadataEntry, MetadataEnvelope }
import net.virtualvoid.fotofinish.process.MetadataProcess.{ AllObjectsReplayed, Metadata, ShuttingDown, StreamEntry }
import net.virtualvoid.fotofinish.util.RepeatSource
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal

trait MetadataJournal {
  def newEntrySink: Sink[MetadataEntry, Any]
  def source(fromSeqNr: Long): Source[StreamEntry, Any]
  def shutdown(): Unit
}

object MetadataJournal {
  /**
   * A flow that produces existing entries and consumes new events to be written to the journal.
   * The flow can be reused.
   */
  def apply(config: RepositoryConfig)(implicit system: ActorSystem): MetadataJournal = {
    import system.dispatcher

    val killSwitch = KillSwitches.shared("kill-journal")
    val seqNrFile = new File(config.metadataDir, "metadata.seqnr.txt")
    def readSeqNr(): Long = Try(scala.io.Source.fromFile(seqNrFile).mkString.toLong).getOrElse(-1)
    def writeSeqNr(last: Long): Unit = {
      // FIXME: what are the performance implications of closing every time?
      val fos = new FileOutputStream(seqNrFile)
      fos.write(last.toString.getBytes())
      fos.close()
    }

    val liveJournalFlow: Flow[MetadataEntry, MetadataEnvelope, Any] =
      Flow[MetadataEntry]
        .statefulMapConcat[MetadataEnvelope] { () =>
          var lastSeqNo: Long = readSeqNr()

          entry =>
            Try {
              val thisSeqNr = lastSeqNo + 1

              val entryWithSeqNr = MetadataEnvelope(thisSeqNr, entry)
              writeJournalEntry(config, entryWithSeqNr)
              // update seqnr last
              writeSeqNr(thisSeqNr)
              lastSeqNo += 1

              entryWithSeqNr :: Nil
            }.recover {
              case ex =>
                println(s"[journal] processing new entry [$entry] failed with [${ex.getMessage}], dropping")
                ex.printStackTrace()
                Nil
            }.get
        }

    def readAllEntries(): Future[Source[MetadataEnvelope, Any]] =
      // add some delay which should help to make sure that live stream is running and connected when we start reading the
      // file
      after(100.millis, system.scheduler) {
        Future.successful {
          if (config.allMetadataFile.exists())
            FileIO.fromPath(config.allMetadataFile.toPath)
              .via(Compression.gunzip())
              .via(Framing.delimiter(ByteString("\n"), 1000000))
              .map(_.utf8String)
              .mapConcat(readJournalEntry(config, _).toVector)
              .recoverWithRetries(1, {
                case z: ZipException => Source.empty
              })
          else
            Source.empty
        }
      }

    // A persisting source that replays all existing journal entries on and on as long as something listens to it.
    // The idea is similar to IP multicast that multiple subscribers can attach to the already running program and
    // the overhead for providing the data is only paid once for all subscribers.
    // Another analogy would be a news ticker on a news channel where casual watchers can come and go at any point in
    // time and would eventually get all the information as everyone else (without requiring a dedicated TV for any watcher).
    val existingEntryWheel: Source[StreamEntry, Any] =
      Source
        .fromGraph(new RepeatSource(
          Source.lazyFutureSource(readAllEntries)
            .map[StreamEntry](Metadata)
            .concat(Source.single(AllObjectsReplayed))
        ))
        .runWith(BroadcastHub.sink(1024))

    // Attaches to the wheel but only returns the journal entries between the given seqNr and AllObjectsReplayed.
    def existingEntriesStartingWith(fromSeqNr: Long): Source[StreamEntry, Any] =
      existingEntryWheel
        .via(new TakeFromWheel(fromSeqNr))

    val (liveSink, liveSource) =
      MergeHub.source[MetadataEntry]
        .via(liveJournalFlow)
        .via(killSwitch.flow)
        .toMat(BroadcastHub.sink[MetadataEnvelope](2048))(Keep.both)
        .run()

    new MetadataJournal {
      override def newEntrySink: Sink[MetadataEntry, Any] = liveSink
      override def source(fromSeqNr: Long): Source[StreamEntry, Any] =
        existingEntriesStartingWith(fromSeqNr)
          .concat(liveSource.map(Metadata))
          .concat(Source.single(ShuttingDown))
      override def shutdown(): Unit = killSwitch.shutdown()
    }
  }

  private def readJournalEntry(config: RepositoryConfig, entry: String): Option[MetadataEnvelope] = {
    import config.entryFormat
    try Some(entry.parseJson.convertTo[MetadataEnvelope])
    catch {
      case NonFatal(ex) =>
        println(s"Couldn't read [$entry] because of ${ex.getMessage}")
        ex.printStackTrace()
        None
    }
  }

  private def writeJournalEntry(config: RepositoryConfig, envelope: MetadataEnvelope): Unit = {
    val fos = new FileOutputStream(config.allMetadataFile, true)
    val out = new GZIPOutputStream(fos)
    import config.entryFormat
    out.write(envelope.toJson.compactPrint.getBytes("utf8"))
    out.write('\n')
    out.close()
    fos.close()
  }

  /** A stage that ignores element from the journal wheel until it finds the first matching  */
  class TakeFromWheel(firstSeqNr: Long) extends GraphStage[FlowShape[StreamEntry, StreamEntry]] {
    val in = Inlet[StreamEntry]("TakeFromWheel.in")
    val out = Outlet[StreamEntry]("TakeFromWheel.out")
    val shape: FlowShape[StreamEntry, StreamEntry] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with OutHandler {
      import MetadataProcess.Metadata

      def onPull(): Unit = pull(in)

      val waitForStart = new InHandler {
        var seenSmaller: Boolean = false
        override def onPush(): Unit =
          grab(in) match {
            case e @ Metadata(entry) =>
              seenSmaller ||= entry.seqNr < firstSeqNr

              if (entry.seqNr == firstSeqNr) {
                push(out, e)
                setHandler(in, transferring)
              } else if (seenSmaller && entry.seqNr > firstSeqNr)
                throw new IllegalStateException(s"Gap in journal before ${entry.seqNr}")
              // else if (!seenSmaller && entry.seqNr > firstSeqNr) // need to wrap first
              // else if (entry.seqNr < firstSeqNr) // seenSmaller was set, continue
              else pull(in)

            case AllObjectsReplayed =>
              if (seenSmaller) { // seq nr not yet available
                push(out, AllObjectsReplayed)
                completeStage()
              } else {
                seenSmaller = true // when wrapping, we treat the end signal as smallest seqNr
                pull(in)
              }
            case x => throw new IllegalStateException(s"Unexpected element $x")
          }
      }
      val transferring = new InHandler {
        override def onPush(): Unit = {
          val el = grab(in)
          push(out, el)
          if (el == AllObjectsReplayed) completeStage()
        }
      }

      setHandler(out, this)
      setHandler(in, waitForStart)
    }
  }
}