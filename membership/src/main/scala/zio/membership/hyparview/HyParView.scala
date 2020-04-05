package zio.membership.hyparview

import zio._
import zio.membership.transport.Transport
import zio.membership.{ Error, SendError, TransportError }
import zio.duration._
import zio.clock.Clock
import zio.stream.{ Stream, Take, ZStream }
import zio.logging.Logging.Logging
import zio.logging.log
import zio.membership.hyparview.ActiveProtocol.PlumTreeProtocol
import zio.keeper.membership.{ ByteCodec, TaggedCodec }

object HyParView {

  def live[R <: Transport[T] with TRandom with Logging with Clock with HyParViewConfig, T: Tagged](
    localAddr: T,
    seedNodes: List[T],
    shuffleSchedule: Schedule[R, ViewState, Any]
  )(
    implicit
    ev1: TaggedCodec[InitialProtocol[T]],
    ev2: TaggedCodec[ActiveProtocol[T]],
    ev3: ByteCodec[JoinReply[T]]
  ): ZLayer[R, Error, PeerService[T]] = {
    type R1 = R with Views[T]
    val layer = ZLayer.identity[R] ++ Views.fromConfig(localAddr)
    layer >>> ZLayer.fromManaged {
      for {
        env <- ZManaged.environment[R1]
        cfg <- getConfig.toManaged_
        _ <- log
              .info(s"Starting HyParView on $localAddr with configuration:\n${cfg.prettyPrint}")
              .toManaged(_ => log.info("Shut down HyParView"))
        scope <- ScopeIO.make
        connections <- Queue
                        .bounded[(T, Chunk[Byte] => IO[TransportError, Unit], Stream[Error, Chunk[Byte]], UIO[_])](
                          cfg.connectionBuffer
                        )
                        .toManaged(_.shutdown)
        plumTreeMessages <- Queue
                             .sliding[Take[Error, (T, PlumTreeProtocol)]](cfg.userMessagesBuffer)
                             .toManaged(_.shutdown)
        peerEvents <- Queue.sliding[PeerEvent[T]](128).toManaged(_.shutdown)
        sendInitial0 = (to: T, msg: InitialProtocol.InitialMessage[T]) =>
          sendInitial[T](to, msg, scope, connections).provide(env)
        _ <- receiveInitialProtocol[R1, Error, T](Transport.bind(localAddr), cfg.concurrentIncomingConnections)
              .merge(ZStream.fromQueue(connections))
              .merge(neighborProtocol[T].scheduleElements(Schedule.spaced(2.seconds)))
              .flatMapParSwitch(cfg.activeViewCapacity) {
                case (addr, send, receive, release) =>
                  ZStream
                    .fromEffect(peerEvents.offer(PeerEvent.NeighborUp(addr)))
                    .ensuring(peerEvents.offer(PeerEvent.NeighborDown(addr)))
                    .flatMap(_ => runActiveProtocol[R1, Error, T](addr, send, sendInitial0)(receive).ensuring(release))
              }
              .into(plumTreeMessages)
              .toManaged_
              .fork
        _ <- periodic
              .doShuffle[T]
              .repeat(shuffleSchedule)
              .toManaged_
              .fork
        _ <- periodic
              .doReport[T]
              .repeat(Schedule.spaced(2.seconds))
              .toManaged_
              .fork
        _ <- ZIO.foreach_(seedNodes)(sendInitial0(_, InitialProtocol.Join(localAddr))).toManaged_
      } yield new PeerService.Service[T] {
        override val identity: ZIO[Any, Nothing, T] =
          ZIO.succeed(localAddr)

        override val getPeers: IO[Nothing, Set[T]] =
          Views.activeView.commit.provide(env)

        override def send(to: T, message: PlumTreeProtocol): IO[SendError, Unit] =
          Views.send(to, message).provide(env)

        override val receive: ZStream[Any, Error, (T, PlumTreeProtocol)] =
          ZStream.fromQueue(plumTreeMessages).unTake

        override val events: ZStream[Any, Nothing, PeerEvent[T]] =
          ZStream.fromQueue(peerEvents)
      }
    }
  }
}
