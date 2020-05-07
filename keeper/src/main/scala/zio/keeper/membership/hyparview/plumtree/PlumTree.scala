package zio.keeper.membership.hyparview.plumtree

import java.util.UUID

import zio._
import zio.clock.Clock
import zio.duration._
import zio.keeper.membership.Membership
import zio.keeper.membership.hyparview.PeerService
import zio.keeper.{ ByteCodec, Error, NodeAddress, SendError }
import zio.logging._
import zio.logging.Logging.Logging
import zio.keeper.membership.hyparview.ActiveProtocol._
import zio.keeper.membership.hyparview.PeerEvent._
import zio.keeper.uuid.makeRandomUUID
import zio.keeper.membership.hyparview.TRandom
import zio.stream.ZStream

import scala.Function.untupled

object PlumTree {

  def make[A: Tagged: ByteCodec](
    initialEagerPeers: Int,
    messagesBuffer: Int,
    gossipBuffer: Int,
    deduplicationBuffer: Int,
    graftMessagesBuffer: Int,
    iHaveInterval: Duration,
    graftInitialDelay: Duration,
    graftTimeout: Duration
  ): ZLayer[Clock with TRandom with PeerService with Logging, Nothing, Membership[A]] = {
    val layer =
      ZLayer.identity[Clock with TRandom with PeerService with Logging] ++
        PeerState.live(initialEagerPeers)

    def processEvents =
      PeerService.events.foreach {
        case NeighborDown(node) =>
          PeerState.removePeer(node).commit
        case NeighborUp(node) =>
          // TODO: double check paper
          PeerState.addToEagerPeers(node).commit
      }

    def processMessages(
      deduplication: BoundedTMap[UUID, Gossip],
      gossipOut: (NodeAddress, UUID) => UIO[Unit],
      userOut: (NodeAddress, Chunk[Byte]) => UIO[Unit],
      onReceive: Either[UUID, (UUID, NodeAddress)] => UIO[Unit]
    ): ZIO[PeerState with Logging with PeerService, Error, Unit] =
      PeerService.receive.foreach {
        case (sender, msg) =>
          msg match {
            case Prune =>
              PeerState.moveToLazyPeers(sender).commit
            case Graft(uuid) =>
              deduplication.get(uuid).commit.flatMap {
                case None => log.warn(s"Expected to find message $uuid but didn't")
                case Some(msg) =>
                  PeerService
                    .send(sender, msg)
                    .foldCauseM(
                      log.error(s"Failed replying to Graft message from $sender", _),
                      _ => log.debug(s"Replied to Graft message from $sender")
                    ) *> PeerState.addToEagerPeers(sender).commit
              }
            case IHave(uuids) =>
              def process(uuid: UUID) =
                deduplication.contains(uuid).commit.flatMap {
                  case true =>
                    log.debug(s"Received IHave for already received message $uuid")
                  case false =>
                    onReceive(Right((uuid, sender)))
                }
              ZIO.foreach_(uuids)(process)
            case UserMessage(payload) =>
              userOut(sender, payload) <* log.debug(s"Received userMessage from $sender")
            case m @ Gossip(uuid, payload) =>
              deduplication.add(uuid, m).commit.flatMap {
                case true =>
                  // first occurrence
                  onReceive(Left(uuid)) *>
                    (PeerState
                      .addToEagerPeers(sender) *> PeerState.eagerPushPeers.zip(PeerState.lazyPushPeers)).commit
                      .flatMap {
                        case (eagerPeers, lazyPeers) =>
                          val sendEager = ZIO.foreach_(eagerPeers.filterNot(_ == sender)) { peer =>
                            PeerService
                              .send(peer, m)
                              .foldCauseM(
                                log.error(s"Failed sending message to $peer", _),
                                _ => log.info(s"Sent Gossip to $peer")
                              )
                          }

                          // no need to filter here as we ensured sender is in eager peers
                          val sendLazy = ZIO.foreach_(lazyPeers.map((_, uuid)))(gossipOut.tupled)

                          sendEager *> sendLazy *> userOut(sender, payload)
                      }
                case false =>
                  // seen before -> disconnect sender
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
          gossip       <- Queue.sliding[(NodeAddress, UUID)](gossipBuffer).toManaged(_.shutdown)
          userMessages <- Queue.sliding[(NodeAddress, Chunk[Byte])](messagesBuffer).toManaged(_.shutdown)
          graftQueue   <- Queue.sliding[Either[UUID, (UUID, NodeAddress)]](graftMessagesBuffer).toManaged(_.shutdown)
          _            <- processEvents.toManaged_.fork
          _ <- processMessages(
                messages,
                untupled((gossip.offer _).andThen(_.unit)),
                untupled((userMessages.offer _).andThen(_.unit)),
                graftQueue.offer(_).unit
              ).toManaged_.fork
          _ <- sendIHave(ZStream.fromQueue(gossip), Schedule.spaced(iHaveInterval)).runDrain.toManaged_.fork
          _ <- sendGraft(ZStream.fromQueue(graftQueue), graftInitialDelay, Schedule.spaced(graftTimeout)).runDrain.toManaged_.fork
        } yield new Membership.Service[A] {
          override val localMember: ZIO[Any, Nothing, NodeAddress] = PeerService.identity.provide(env)
          override val nodes: ZIO[Any, Nothing, Set[NodeAddress]]  = PeerService.getPeers.provide(env)
          override def send(data: A, receipt: NodeAddress): ZIO[Any, SendError, Unit] = {
            for {
              chunk <- ByteCodec.decode(data).mapError(SendError.SerializationFailed)
              _     <- PeerService.send(receipt, UserMessage(chunk))
            } yield ()
          }.provide(env)

          override def broadcast(data: A): ZIO[Any, SendError, Unit] = {
            ByteCodec.decode(data).mapError(SendError.SerializationFailed(_)).flatMap { chunk =>
              makeRandomUUID.flatMap { uuid =>
                val msg = Gossip(uuid, chunk)
                PeerState.eagerPushPeers
                  .zip(PeerState.lazyPushPeers)
                  .commit
                  .flatMap {
                    case (eagerPeers, lazyPeers) =>
                      val sendEager = ZIO.foreach_(eagerPeers) { peer =>
                        PeerService
                          .send(peer, Gossip(uuid, chunk))
                          .foldCauseM(
                            log.error(s"Failed sending message to $peer", _),
                            _ => log.info(s"Sent Gossip to $peer")
                          )
                      }

                      // no need to filter here as we ensured sender is in eager peers
                      val sendLazy = ZIO.foreach_(lazyPeers.map((_, uuid)))(gossip.offer)
                      messages.add(uuid, msg).commit *> sendEager *> sendLazy
                  }
              }
            }
          }.provide(env)

          override def receive: ZStream[Any, Nothing, (NodeAddress, A)] =
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
