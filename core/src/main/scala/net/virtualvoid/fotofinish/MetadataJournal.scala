package net.virtualvoid.fotofinish

import akka.actor.typed.scaladsl.{ Behaviors, StashBuffer }
import akka.actor.typed.{ ActorRef, ActorSystem, Behavior, PostStop }
import akka.stream.scaladsl.Source
import akka.util.Timeout
import net.virtualvoid.fotofinish.metadata.MetadataEntry

import scala.collection.immutable.TreeSet
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

trait MetadataStorageBackend {
  def store(entry: MetadataEntry): Future[MetadataEntry]
  def close(): Unit

  def stream(fromSeq: Long): Source[MetadataEntry, Any]
}

object MetadataJournal {
  sealed trait Command
  final case class Store(entry: MetadataEntry, replyTo: ActorRef[Stored]) extends Command
  final case class Stored(entry: MetadataEntry)

  final case class Replay(to: ActorRef[ReplayStream], fromSeq: Long = 0L) extends Command
  final case class ReplayStream(stream: Source[MetadataEntry, Any])

  final case class Subscribe(actor: ActorRef[JournalEvent], fromSeq: Long = 0L) extends Command
  final case class JournalEvent(entry: MetadataEntry)

  private sealed trait InternalCommand extends Command
  private final case class BackendStored(storageResult: Try[MetadataEntry], replyTo: ActorRef[Stored]) extends InternalCommand

  def journal(backend: MetadataStorageBackend): Behavior[Command] =
    Behaviors.setup { ctx =>
      def withSubscribers(subs: Set[ActorRef[JournalEvent]]): Behavior[Command] =
        Behaviors
          .receiveMessage[Command] {
            case Store(entry, replyTo) =>
              ctx.pipeToSelf(backend.store(entry))(BackendStored(_, replyTo))
              Behaviors.same

            case BackendStored(Success(entry), replyTo) =>
              replyTo ! Stored(entry)
              val ev = JournalEvent(entry)
              subs.foreach(_ ! ev)
              Behaviors.same

            case Replay(to, from) =>
              to ! ReplayStream(backend.stream(from))
              Behaviors.same

            case Subscribe(whom, from) =>
              // FIXME: don't ignore `from`
              withSubscribers(subs + whom)
          }
          .receiveSignal {
            case (_, PostStop) =>
              backend.close()
              Behaviors.same
          }

      withSubscribers(Set.empty)
    }
}

trait RepositoryAPI {
  def allObjects(implicit timeout: Timeout): Source[Hash, Any]
}
object RepositoryAPI {
  def apply(journal: ActorRef[MetadataJournal.Command])(implicit system: ActorSystem[_]): RepositoryAPI = {
    sealed trait Command
    final case class GetAllObjects(replyTo: ActorRef[AllObjects]) extends Command
    // providing a Source might allow a different implementation later on
    final case class AllObjects(objectsSource: Source[Hash, Any], lastSeqNr: Long)

    final case class GotReplay(stream: Source[MetadataEntry, Any]) extends Command
    final case class NewEntry(entry: MetadataEntry) extends Command

    final case class ReplayFinished(result: Try[(TreeSet[Hash], Long)]) extends Command

    import akka.actor.typed.scaladsl.AskPattern._
    import system.executionContext

    def initial: Behavior[Command] =
      Behaviors.setup { ctx =>
        val replayWrapper = ctx.messageAdapter[MetadataJournal.ReplayStream](res => GotReplay(res.stream))
        val newEntryWrapper = ctx.messageAdapter[MetadataJournal.JournalEvent](res => NewEntry(res.entry))

        journal ! MetadataJournal.Replay(replayWrapper)

        def waitingForReplay(): Behavior[Command] =
          Behaviors.withStash(100) { buffer =>
            Behaviors.receiveMessagePartial {
              case GotReplay(stream) =>
                stream
                  .runFold((TreeSet.empty[Hash], -1L)) { (cur, next) =>
                    (cur._1 + next.header.forData, next.header.seqNr)
                  }
                  .onComplete(res => ctx.self ! ReplayFinished(res))

                replaying(buffer)
              case g: GetAllObjects =>
                buffer.stash(g)
                Behaviors.same
            }
          }

        def replaying(buffer: StashBuffer[Command]): Behavior[Command] =
          Behaviors.receiveMessagePartial {
            case ReplayFinished(Success((hashes, lastSeqNr))) =>
              journal ! MetadataJournal.Subscribe(newEntryWrapper, lastSeqNr + 1)

              buffer.unstashAll(subscribed(hashes, lastSeqNr + 1))
            case ReplayFinished(Failure(cause)) =>
              println("Replaying failed")
              cause.printStackTrace()
              Behaviors.stopped
            case g: GetAllObjects =>
              buffer.stash(g)
              Behaviors.same
          }

        def subscribed(knownHashes: TreeSet[Hash], lastSeqNr: Long): Behavior[Command] =
          Behaviors.receiveMessagePartial {
            case NewEntry(entry) =>
              require(entry.header.seqNr == lastSeqNr + 1) // as long as we read all events
              subscribed(knownHashes + entry.header.forData, entry.header.seqNr)

            case GetAllObjects(replyTo) =>
              replyTo ! AllObjects(Source(knownHashes), lastSeqNr)
              Behaviors.same
          }

        waitingForReplay()
      }

    val ref = system.systemActorOf(initial, "repository-api")

    new RepositoryAPI {
      override def allObjects(implicit timeout: Timeout): Source[Hash, Any] =
        Source.futureSource(ref.ask(GetAllObjects).map(_.objectsSource))
    }
  }
}