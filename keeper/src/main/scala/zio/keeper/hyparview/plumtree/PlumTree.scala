package zio.keeper.hyparview.plumtree

import java.util.UUID

import zio._
import zio.clock.Clock
import zio.duration._
import zio.keeper.hyparview.{ PeerService, TRandom }
import zio.keeper._
import zio.logging._
import zio.logging.Logging
import zio.keeper.hyparview.PeerEvent._
import zio.keeper.hyparview.Round
import zio.keeper.uuid.makeRandomUUID
import zio.stream.{ Stream, ZStream }
import zio.keeper.hyparview.Message._

import scala.Function.untupled

object PlumTree {

  def make[A: Tag: ByteCodec](
    initialEagerPeers: Int,
    messagesBuffer: Int,
    gossipBuffer: Int,
    deduplicationBuffer: Int,
    graftMessagesBuffer: Int,
    iHaveInterval: Duration,
    graftInitialDelay: Duration,
    graftTimeout: Duration,
    optimizeThreshold: Int
  ): ZLayer[Clock with TRandom with PeerService with Logging, Nothing, Membership[A]] = {
    val layer =
      ZLayer.identity[Clock with TRandom with PeerService with Logging] ++
        PeerState.live(initialEagerPeers)

    def processEvents(
      deduplication: BoundedTMap[UUID, Gossip],
      gossipOut: (NodeAddress, UUID, Round) => UIO[Unit],
      userOut: (NodeAddress, Chunk[Byte]) => UIO[Unit],
      onReceive: Either[(UUID, Round, NodeAddress), (UUID, Round, NodeAddress)] => UIO[Unit]
    ) =
      PeerService.events.foreach {
        case NeighborDown(node) =>
          PeerState.removePeer(node).commit
        case NeighborUp(node) =>
          // TODO: double check paper
          PeerState.addToEagerPeers(node).commit
        case MessageReceived(sender, msg) =>
          msg match {
            case Prune =>
              PeerState.moveToLazyPeers(sender).commit
            case Graft(uuid) =>
              PeerState.addToEagerPeers(sender).commit *>
                deduplication.get(uuid).commit.flatMap {
                  case None =>
                    log.warn(s"Expected to find message $uuid but didn't")
                  case Some(msg) =>
                    PeerService
                      .send(sender, msg)
                      .foldCauseM(
                        log.error(s"Failed replying to Graft message from $sender", _),
                        _ => log.debug(s"Replied to Graft message from $sender")
                      )
                }
            case IHave(messages) =>
              def process(uuid: UUID, round: Round) =
                deduplication.contains(uuid).commit.flatMap {
                  case true =>
                    log.debug(s"Received IHave for already received message $uuid")
                  case false =>
                    onReceive(Right((uuid, round, sender)))
                }
              ZIO.foreach_(messages)((process _).tupled)
            case UserMessage(payload) =>
              userOut(sender, payload) <* log.debug(s"Received userMessage from $sender")
            case m @ Gossip(uuid, payload, round) =>
              deduplication.add(uuid, m).commit.flatMap {
                case true =>
                  // first occurrence
                  onReceive(Left((uuid, round, sender))) *>
                    (PeerState
                      .addToEagerPeers(sender) *> PeerState.eagerPushPeers.zip(PeerState.lazyPushPeers)).commit
                      .flatMap {
                        case (eagerPeers, lazyPeers) =>
                          val sendEager = ZIO.foreach_(eagerPeers.filterNot(_ == sender)) { peer =>
                            PeerService
                              .send(peer, m.copy(round = round.inc))
                              .foldCauseM(
                                log.error(s"Failed sending message to $peer", _),
                                _ => log.info(s"Sent Gossip to $peer")
                              )
                          }

                          // no need to filter here as we ensured sender is in eager peers
                          val sendLazy = ZIO.foreach_(lazyPeers.map((_, uuid, round.inc)))(gossipOut.tupled)

                          sendEager *> sendLazy *> userOut(sender, payload)
                      }
                case false =>
                  // seen before -> disconnect sender
                  PeerState.moveToLazyPeers(sender).commit *>
                    PeerService
                      .send(sender, Prune)
                      .foldCauseM(
                        log.error(s"Failed to send prune message to $sender", _),
                        _ => log.debug(s"Sent prune message to $sender")
                      )
              }
          }
      }
    layer >>>
      ZLayer.fromManaged {
        for {
          env          <- ZManaged.environment[TRandom with PeerService with Logging with PeerState]
          messages     <- BoundedTMap.make[UUID, Gossip](deduplicationBuffer).toManaged_
          gossip       <- Queue.sliding[(NodeAddress, UUID, Round)](gossipBuffer).toManaged(_.shutdown)
          userMessages <- Queue.sliding[(NodeAddress, Chunk[Byte])](messagesBuffer).toManaged(_.shutdown)
          graftQueue <- Queue
                         .sliding[Either[(UUID, Round, NodeAddress), (UUID, Round, NodeAddress)]](graftMessagesBuffer)
                         .toManaged(_.shutdown)
          _ <- processEvents(
                messages,
                untupled((gossip.offer _).andThen(_.unit)),
                untupled((userMessages.offer _).andThen(_.unit)),
                graftQueue.offer(_).unit
              ).toManaged_.fork
          _ <- sendIHave(ZStream.fromQueue(gossip), Schedule.spaced(iHaveInterval)).runDrain.toManaged_.fork
          _ <- sendGraft(
                ZStream.fromQueue(graftQueue),
                graftInitialDelay,
                Schedule.spaced(graftTimeout),
                optimizeThreshold
              ).runDrain.toManaged_.fork
        } yield new Membership.Service[A] {
          override def send(data: A, receipt: NodeAddress): UIO[Unit] = {
            (for {
              chunk <- ByteCodec.encode(data).mapError(SendError.SerializationFailed)
              _     <- PeerService.send(receipt, UserMessage(chunk))
            } yield ()).orDie
          }.provide(env)

          override def broadcast(data: A): UIO[Unit] = {
            ByteCodec
              .encode(data)
              .mapError(SendError.SerializationFailed(_))
              .flatMap { chunk =>
                makeRandomUUID.flatMap { uuid =>
                  val round = Round.zero
                  val msg   = Gossip(uuid, chunk, round)
                  PeerState.eagerPushPeers
                    .zip(PeerState.lazyPushPeers)
                    .commit
                    .flatMap {
                      case (eagerPeers, lazyPeers) =>
                        val sendEager = ZIO.foreach_(eagerPeers) { peer =>
                          PeerService
                            .send(peer, msg)
                            .foldCauseM(
                              log.error(s"Failed sending message to $peer", _),
                              _ => log.info(s"Sent Gossip to $peer")
                            )
                        }

                        // no need to filter here as we ensured sender is in eager peers
                        val sendLazy = ZIO.foreach_(lazyPeers.map((_, uuid, round)))(gossip.offer)
                        messages.add(uuid, msg).commit *> sendEager *> sendLazy
                    }
                }
              }
              .orDie
          }.provide(env)

          override def receive: Stream[Nothing, (NodeAddress, A)] =
            ZStream
              .fromQueue(userMessages)
              .mapM {
                case (sender, chunk) =>
                  ByteCodec[A]
                    .fromChunk(chunk)
                    .foldCauseM(
                      log.error(s"Failed to read user message from $sender", _).as(None),
                      m => ZIO.succeed(Some((sender, m)))
                    )
              }
              .collect {
                case Some((addr, msg)) => (addr, msg)
              }
              .provide(env)
        }
      }
  }
}
