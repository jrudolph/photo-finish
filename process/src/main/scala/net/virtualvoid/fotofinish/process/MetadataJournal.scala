package net.virtualvoid.fotofinish.process

import java.io.{ File, FileOutputStream }
import java.util.zip.{ GZIPOutputStream, ZipException }

import akka.actor.ActorSystem
import akka.stream.{ Attributes, FlowShape, Inlet, KillSwitches, Outlet }
import akka.stream.scaladsl.{ BroadcastHub, Compression, FileIO, Flow, Framing, Keep, MergeHub, Sink, Source }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.util.ByteString
import net.virtualvoid.fotofinish.metadata.{ MetadataEntry, MetadataEnvelope }
import net.virtualvoid.fotofinish.process.MetadataProcess.{ AllObjectsReplayed, Metadata, ShuttingDown, StreamEntry }
import spray.json._

import scala.concurrent.Future
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
  def apply(config: ProcessConfig)(implicit system: ActorSystem): MetadataJournal = {
    import system.dispatcher
    import config.envelopeFormat

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

    val readEntryFlow: Flow[ByteString, MetadataEnvelope, Any] =
      Flow[ByteString]
        .via(Compression.gunzip())
        .via(Framing.delimiter(ByteString("\n"), 1000000))
        // batch and async loading
        .grouped(100) // TODO: make configurable?
        .mapAsync(8) { lines =>
          Future {
            lines
              .flatMap(bs => readJournalEntry(bs.utf8String))
          }
        }
        .mapConcat(identity)
        .recoverWithRetries(1, {
          case z: ZipException => Source.empty
        })

    def allMetadataSourceOrEmpty(offset: Long): Source[MetadataEnvelope, Any] =
      if (config.allMetadataFile.exists())
        FileIO.fromPath(config.allMetadataFile.toPath, chunkSize = 8192, startPosition = offset)
          .via(readEntryFlow)
      else
        Source.empty

    def readFrom(seqNr: Long): Source[StreamEntry, Any] = {
      val startByte: Future[Long] =
        if (config.metadataIndexFile.exists)
          FileIO.fromPath(config.metadataIndexFile.toPath)
            .via(Framing.delimiter(ByteString("\n"), 1000000))
            .map { line =>
              val Vector(seqNr, offset) = line.utf8String.split(" ").map(_.toLong).toVector
              (seqNr.toLong, offset.toLong)
            }
            // FIXME here
            .takeWhile(_._1 <= seqNr)
            .runWith(Sink.lastOption)
            .map(_.map(_._2).getOrElse(0L))
            .recover { case _ => 0L } // e.g. if reading index file failed for another reason
        else
          Future.successful(0L)

      val source = startByte.map(allMetadataSourceOrEmpty)
      Source.futureSource(source)
        .map[StreamEntry](Metadata)
        .concat(Source.single(AllObjectsReplayed))
    }

    // Not needed currently
    /*
    import akka.pattern.after
    import scala.concurrent.duration._

    def readAllEntries(): Future[Source[MetadataEnvelope, Any]] =
      // add some delay which should help to make sure that live stream is running and connected when we start reading the
      // file
      after(100.millis, system.scheduler) {
        Future.successful(allMetadataSourceOrEmpty())
      }*/

    // The simplest way of reading existing entries. Main problem: always reads all entries from the beginning of time.
    /*val existingEntriesSimple: Source[StreamEntry, Any] =
      Source.lazyFutureSource(readAllEntries _)
        .map[StreamEntry](Metadata)
        .concat(Source.single(AllObjectsReplayed))*/

    // A persisting source that replays all existing journal entries on and on as long as something listens to it.
    // The idea is similar to IP multicast that multiple subscribers can attach to the already running program and
    // the overhead for providing the data is only paid once for all subscribers.
    // Another analogy would be a news ticker on a news channel where casual watchers can come and go at any point in
    // time and would eventually get all the information as everyone else (without requiring a dedicated TV for any watcher).
    //
    // Currently unused, because it is only helpful during complete rebuilds, otherwise, it has two issues:
    //   * head-of-line blocking if the BroadcastHub buffer is exhausted
    //   * always starts at the beginning, so regardless of where you want to start, you'll always have
    //     to read everything
    /*
    import net.virtualvoid.fotofinish.util.RepeatSource
    val existingEntryWheel: Source[StreamEntry, Any] =
      Source
        .fromGraph(new RepeatSource(existingEntriesSimple))
        .runWith(BroadcastHub.sink(2048))*/

    // Attaches to the wheel but only returns the journal entries between the given seqNr and AllObjectsReplayed.
    def existingEntriesStartingWith(fromSeqNr: Long): Source[StreamEntry, Any] =
      readFrom(fromSeqNr)
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
          .map {
            case orig @ Metadata(env) =>
              val newEntry = config.metadataMapper(env.entry)
              if (env.entry != newEntry) Metadata(MetadataEnvelope(env.seqNr, newEntry))
              else orig
            case x => x
          }
      override def shutdown(): Unit = killSwitch.shutdown()
    }
  }

  private def readJournalEntry(entry: String)(implicit envelopeFormat: JsonFormat[MetadataEnvelope]): Option[MetadataEnvelope] = {
    try Some(entry.parseJson.convertTo[MetadataEnvelope])
    catch {
      case NonFatal(ex) =>
        println(s"Couldn't read [$entry] because of ${ex.getMessage}")
        ex.printStackTrace()
        None
    }
  }

  private def writeJournalEntry(config: ProcessConfig, envelope: MetadataEnvelope): Unit = {
    if (envelope.seqNr % 1000 == 0) writeJournalIndex(config, envelope) // FIXME: make configurable
    val fos = new FileOutputStream(config.allMetadataFile, true)
    val out = new GZIPOutputStream(fos)
    import config.entryFormat
    out.write(envelope.toJson.compactPrint.getBytes("utf8"))
    out.write('\n')
    out.close()
    fos.close()
  }
  private def writeJournalIndex(config: ProcessConfig, envelope: MetadataEnvelope): Unit = {
    val curSize = config.allMetadataFile.length
    val fos = new FileOutputStream(config.metadataIndexFile, true)
    fos.write(f"${envelope.seqNr}%d $curSize%d\n".getBytes)
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

      private object WaitForStart extends InHandler {
        var seenSmaller: Boolean = false
        override def onPush(): Unit =
          grab(in) match {
            case e @ Metadata(entry) =>
              seenSmaller ||= entry.seqNr < firstSeqNr

              if (entry.seqNr == firstSeqNr) {
                push(out, e)
                setHandler(in, Transferring)
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
      private object Transferring extends InHandler {
        override def onPush(): Unit = {
          val el = grab(in)
          push(out, el)
          if (el == AllObjectsReplayed) completeStage()
        }
      }

      setHandler(out, this)
      setHandler(in, WaitForStart)
    }
  }
}