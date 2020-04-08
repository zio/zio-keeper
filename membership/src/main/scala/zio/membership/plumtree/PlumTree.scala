package zio.membership.plumtree

import java.util.UUID

import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging._
import zio.keeper.membership.ByteCodec
import zio.logging.Logging.Logging
import zio.membership.hyparview.ActiveProtocol._
import zio.membership.PeerEvent._
import zio.membership.{ Membership, PeerService, SendError }
import zio.membership.uuid.makeRandomUUID
import zio.membership.hyparview.TRandom
import zio.stream.ZStream

import scala.Function.untupled

object PlumTree {

  def make[T: Tagged, A: Tagged](
    initialEagerPeers: Int,
    messagesBuffer: Int,
    gossipBuffer: Int,
    deduplicationBuffer: Int,
    graftMessagesBuffer: Int,
    iHaveInterval: Duration,
    graftInitialDelay: Duration,
    graftTimeout: Duration
  ): ZLayer[Clock with TRandom with PeerService[T] with Logging, Nothing, Membership[T, A]] = {
    val layer =
      ZLayer.identity[Clock with TRandom with PeerService[T] with Logging] ++
        PeerState.live[T](initialEagerPeers)

    def processEvents =
      PeerService.events[T].foreach {
        case NeighborDown(node) =>
          PeerState.removePeer(node).commit
        case NeighborUp(node) =>
          // TODO: double check paper
          PeerState.addToEagerPeers(node).commit
      }

    def processMessages(
      deduplication: BoundedTMap[UUID, Gossip],
      gossipOut: (T, UUID) => UIO[Unit],
      userOut: Chunk[Byte] => UIO[Unit],
      onReceive: Either[UUID, (UUID, T)] => UIO[Unit]
    ): ZIO[PeerState[T] with Logging with PeerService[T], membership.Error, Unit] =
      PeerService.receive[T].foreach {
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
              userOut(payload) <* log.debug(s"Received userMessage from $sender")
            case m @ Gossip(uuid, payload) =>
              deduplication.add(uuid, m).commit.flatMap {
                case true =>
                  // first occurrence
                  onReceive(Left(uuid)) *>
                    (PeerState
                      .addToEagerPeers(sender) *> PeerState.eagerPushPeers[T].zip(PeerState.lazyPushPeers[T])).commit
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

                          sendEager *> sendLazy *> userOut(payload)
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
          env          <- ZManaged.environment[TRandom with PeerService[T] with Logging with PeerState[T]]
          messages     <- BoundedTMap.make[UUID, Gossip](deduplicationBuffer).toManaged_
          gossip       <- Queue.sliding[(T, UUID)](gossipBuffer).toManaged(_.shutdown)
          userMessages <- Queue.sliding[Chunk[Byte]](messagesBuffer).toManaged(_.shutdown)
          graftQueue   <- Queue.sliding[Either[UUID, (UUID, T)]](graftMessagesBuffer).toManaged(_.shutdown)
          _            <- processEvents.toManaged_.fork
          _ <- processMessages(
                messages,
                untupled((gossip.offer _).andThen(_.unit)),
                userMessages.offer(_).unit,
                graftQueue.offer(_).unit
              ).toManaged_.fork
          _ <- sendIHave(ZStream.fromQueue(gossip), Schedule.spaced(iHaveInterval)).runDrain.toManaged_.fork
          _ <- sendGraft(ZStream.fromQueue(graftQueue), graftInitialDelay, Schedule.spaced(graftTimeout)).runDrain.toManaged_.fork
        } yield new Membership.Service[T, A] {
          override val identity: ZIO[Any, Nothing, T]   = PeerService.identity[T].provide(env)
          override val nodes: ZIO[Any, Nothing, Set[T]] = PeerService.getPeers[T].provide(env)
          override def send(to: T, payload: A)(implicit ev: ByteCodec[A]): ZIO[Any, SendError, Unit] = {
            for {
              chunk <- ByteCodec.toChunk(payload).mapError(SendError.SerializationFailed)
              _     <- PeerService.send(to, UserMessage(chunk))
            } yield ()
          }.provide(env)

          override def broadcast(payload: A)(implicit ev: ByteCodec[A]): ZIO[Any, SendError, Unit] = {
            ByteCodec.toChunk(payload).mapError(SendError.SerializationFailed(_)).flatMap { chunk =>
              makeRandomUUID.flatMap { uuid =>
                val msg = Gossip(uuid, chunk)
                PeerState
                  .eagerPushPeers[T]
                  .zip(PeerState.lazyPushPeers[T])
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

          override def receive(implicit ev: ByteCodec[A]): ZStream[Any, Nothing, A] =
            ZStream
              .fromQueue(userMessages)
              .mapM { chunk =>
                ByteCodec[A]
                  .fromChunk(chunk)
                  .foldCauseM(
                    log.error("Failed to read user message", _).as(None),
                    m => ZIO.succeed(Some(m))
                  )
              }
              .collect {
                case Some(msg) => msg
              }
              .provide(env)
        }
      }
  }
}
