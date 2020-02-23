package net.virtualvoid.fotofinish
package process

import java.io.File

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{ Flow, Keep, MergeHub, Sink, Source }
import net.virtualvoid.fotofinish.metadata._
import net.virtualvoid.fotofinish.util.StatefulDetachedFlow

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success, Try }

trait HandleWithStateFunc[S] {
  def apply[T](f: S => (S, Vector[WorkEntry], T)): Future[T]
  def access[T](f: S => T): Future[T] =
    apply { state =>
      (state, Vector.empty, f(state))
    }

  def handleStream: Sink[S => (S, Vector[WorkEntry]), Any]
}

case class Snapshot[S](processId: String, processVersion: Int, currentSeqNr: Long, state: S)

trait MetadataProcess {
  type S
  type Api

  def id: String = getClass.getName
  def version: Int

  def initialState: S
  def processEvent(state: S, event: MetadataEnvelope): S
  def createWork(state: S, context: ExtractionContext): (S, Vector[WorkEntry])
  def api(handleWithState: HandleWithStateFunc[S])(implicit ec: ExecutionContext): Api

  /** Allows to prepare state loaded from snapshot */
  def initializeStateSnapshot(state: S): S = state

  def saveSnapshot(target: File, config: RepositoryConfig, snapshot: Snapshot[S]): S
  def loadSnapshot(target: File, config: RepositoryConfig)(implicit system: ActorSystem): Option[Snapshot[S]]
}

object MetadataProcess {
  sealed trait StreamEntry
  final case class Metadata(entry: MetadataEnvelope) extends StreamEntry
  case object AllObjectsReplayed extends StreamEntry
  case object MakeSnapshot extends StreamEntry
  case object ShuttingDown extends StreamEntry
  final case class Execute[S, T](f: S => (S, Vector[WorkEntry], T), promise: Promise[T]) extends StreamEntry

  def asSource(p: MetadataProcess, config: RepositoryConfig, journal: MetadataJournal, extractionEC: ExecutionContext)(implicit system: ActorSystem, ec: ExecutionContext): Source[WorkEntry, p.Api] = {
    val injectApi: Source[StreamEntry, p.Api] =
      Source.queue[StreamEntry](10000, OverflowStrategy.dropNew) // FIXME: will this be enough for mass injections?
        .mergeMat(MergeHub.source[StreamEntry])(Keep.both)
        .mapMaterializedValue {
          case (queue, sink) =>
            p.api(new HandleWithStateFunc[p.S] {
              def apply[T](f: p.S => (p.S, Vector[WorkEntry], T)): Future[T] = {
                val promise = Promise[T]
                queue.offer(Execute(f, promise))
                  .onComplete {
                    case Success(QueueOfferResult.Enqueued) =>
                    case x                                  => println(s"Injecting entries failed because of $x")
                  }
                promise.future
              }

              def handleStream: Sink[p.S => (p.S, Vector[WorkEntry]), Any] =
                Flow[p.S => (p.S, Vector[WorkEntry])]
                  .map { f =>
                    Execute[p.S, Unit]({ state =>
                      val (newState, ses) = f(state)
                      (newState, ses, ())
                    }, Promise.successful(()))
                  }
                  .to(sink)
            })
        }

    val extractionContext = new ExtractionContext {
      def executionContext: ExecutionContext = extractionEC
      def accessData[T](hash: Hash)(f: File => Future[T]): Future[T] =
        // FIXME: easy for now as we expect all hashes to be available as files
        f(config.fileInfoOf(hash).repoFile)
    }

    type Handler = ProcessState => StreamEntry => ProcessState
    case class ProcessState(seqNr: Long, processState: p.S, handler: Handler, workEntries: Vector[WorkEntry], lastSnapshotAt: Long, hasFinishedReplaying: Boolean, finished: Boolean) {
      def run(streamEntry: StreamEntry): ProcessState = handler(this)(streamEntry)

      def runEntry(envelope: MetadataEnvelope): ProcessState = copy(processState = p.processEvent(processState, envelope))

      def execute[T](e: Execute[p.S, T]): ProcessState =
        Try(e.f(processState)) match {
          case Success((newState, workEntries, res)) =>
            e.promise.trySuccess(res)
            this
              .withState(newState)
              .addWorkEntries(workEntries)
          case Failure(ex) =>
            e.promise.tryFailure(ex)
            this
        }

      def withSeqNr(newSeqNr: Long): ProcessState = copy(seqNr = newSeqNr)
      def withHandler(newHandler: Handler): ProcessState = copy(handler = newHandler)
      def withState(newState: p.S): ProcessState = copy(processState = newState)
      def withLastSnapshotAt(newValue: Long): ProcessState = copy(lastSnapshotAt = newValue)
      def withLastSnapshotNow: ProcessState = copy(lastSnapshotAt = seqNr)
      def addWorkEntries(newWorkEntries: Vector[WorkEntry]): ProcessState = copy(workEntries = workEntries ++ newWorkEntries)
      def clearWorkEntries: ProcessState = copy(workEntries = Vector.empty)

      def emit: (ProcessState, Vector[WorkEntry]) =
        if (hasFinishedReplaying)
          if (workEntries.nonEmpty) (clearWorkEntries, workEntries)
          else {
            val (newState, workEntries) = p.createWork(processState, extractionContext)
            (withState(newState), workEntries)
          }
        else
          (this, Vector.empty)

      def setFinishedReplaying: ProcessState = copy(hasFinishedReplaying = true)
      def setFinished: ProcessState = copy(finished = true)

      def saveSnapshot(force: Boolean = false): ProcessState =
        if ((lastSnapshotAt + config.snapshotOffset < seqNr) || (force && lastSnapshotAt < seqNr)) bench("Serializing state") {
          val newState = p.saveSnapshot(processSnapshotFile(p, config), config, Snapshot(p.id, p.version, seqNr, processState))
          this
            .withLastSnapshotNow
            .withState(newState)
        }
        else this
    }
    def bench[T](what: String)(f: => T): T = {
      val started = System.nanoTime()
      val res = f
      val lasted = System.nanoTime() - started
      println(s"[${p.id}] $what in ${lasted / 1000000} ms")
      res
    }
    def replaying(waitingExecutions: Vector[Execute[p.S, _]]): Handler = state => {
      case Metadata(e) =>
        if (e.seqNr == state.seqNr + 1) {
          state
            .runEntry(e)
            .withSeqNr(e.seqNr)
        } else if (e.seqNr < state.seqNr + 1)
          throw new IllegalStateException(s"Got unexpected duplicate seqNr ${e.seqNr} after last ${state.seqNr}")
        else
          throw new IllegalStateException(s"Got unexpected gap in seqNr ${e.seqNr} after last ${state.seqNr}")

      case AllObjectsReplayed =>
        println(s"[${p.id}] finished replaying after [${state.seqNr}]")

        // run all waiting executions
        waitingExecutions.foldLeft(state)(_.execute(_))
          .saveSnapshot()
          .setFinishedReplaying
          .withHandler(ignoreDuplicateSeqNrs)

      case e: Execute[p.S, t] @unchecked =>
        state.withHandler(replaying(waitingExecutions :+ e))

      case MakeSnapshot => state.saveSnapshot()
      case ShuttingDown => state.saveSnapshot(force = true).setFinished
    }
    def ignoreDuplicateSeqNrs: Handler = state => {
      case Metadata(e) if e.seqNr <= state.seqNr =>
        println(s"[${p.id}] at ${state.seqNr} got ${e.seqNr}. Ignoring")
        state
      case m: Metadata =>
        state
          .withHandler(liveEvents)
          .run(m)

      case e: Execute[p.S, t] @unchecked => state.execute(e)

      case MakeSnapshot                  => state.saveSnapshot()
      case ShuttingDown                  => state.saveSnapshot(force = true).setFinished
    }
    def liveEvents: Handler = state => {
      case Metadata(e) =>
        if (e.seqNr == state.seqNr + 1) {
          state
            .runEntry(e)
            .withSeqNr(e.seqNr)
        } else if (e.seqNr < state.seqNr + 1)
          throw new IllegalStateException(s"Got unexpected duplicate seqNr ${e.seqNr} after last ${state.seqNr}, ignoring...")
        else
          throw new IllegalStateException(s"Got unexpected gap in seqNr ${e.seqNr} after last ${state.seqNr}")

      case e: Execute[p.S, t] @unchecked => state.execute(e)

      case MakeSnapshot                  => state.saveSnapshot()
      case ShuttingDown                  => state.saveSnapshot(force = true).setFinished

      case AllObjectsReplayed            => throw new IllegalStateException("Unexpected AllObjectsReplayed in state liveEvents")
    }

    def statefulDetachedFlow[T, U, S](initialState: () => S, handle: (S, T) => S, emit: S => (S, Vector[U]), isFinished: S => Boolean): Flow[T, U, Any] =
      Flow.fromGraph(new StatefulDetachedFlow(initialState, handle, emit, isFinished))

    val snapshotFile = processSnapshotFile(p, config)
    val snapshot =
      bench("Deserializing state") { Try(p.loadSnapshot(snapshotFile, config)) }
        .recover {
          case ex =>
            println(s"[${p.id}] Loading snapshot from [$snapshotFile] failed with $ex, replacing snapshot and replaying from scratch")
            snapshotFile.renameTo(new File(snapshotFile.getAbsolutePath + ".bak"))

            ex.printStackTrace()
            None
        }
        .get
        .getOrElse(Snapshot(p.id, p.version, -1L, p.initialState))

    println(s"[${p.id}] initialized process at seqNr [${snapshot.currentSeqNr}]")

    val processFlow =
      Flow[StreamEntry]
        .merge(Source.tick(config.snapshotInterval, config.snapshotInterval, MakeSnapshot))
        .mergeMat(injectApi)(Keep.right)
        .via(statefulDetachedFlow[StreamEntry, WorkEntry, ProcessState](
          () => ProcessState(snapshot.currentSeqNr, snapshot.state, replaying(Vector.empty), Vector.empty, lastSnapshotAt = snapshot.currentSeqNr, hasFinishedReplaying = false, finished = false),
          _.run(_),
          _.emit,
          _.finished
        ))
        .recoverWithRetries(1, {
          case ex =>
            println(s"Process for [${p.id}] failed with [${ex.getMessage}]. Aborting the process.")
            ex.printStackTrace()
            Source.empty
        })

    journal.source(snapshot.currentSeqNr + 1)
      .map {
        case m @ Metadata(entry) =>
          if ((entry.seqNr % 100) == 0) println(s"[${p.id}] at [${entry.seqNr}]")

          m
        case e =>
          println(s"[${p.id}] at [$e]")
          e
      }
      .viaMat(processFlow)(Keep.right)
  }

  // FIXME: join with header
  private def processSnapshotFile(p: MetadataProcess, config: RepositoryConfig): File =
    new File(config.snapshotDir, s"${p.id.replaceAll("""[\[\]]""", "_")}.snapshot")
}
