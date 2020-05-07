package zio.keeper.membership.hyparview

import zio.clock.Clock
import zio.keeper.transport.Transport
import zio.keeper.{ Error, NodeAddress, SendError, TransportError }
import zio.logging.Logging
import zio.logging.log
import zio.stream.{ Stream, Take, ZStream }
import zio._
import zio.keeper.membership.hyparview.ActiveProtocol._
import zio.duration._

object HyParView {

  def live[R <: Transport with TRandom with Logging with Clock with HyParViewConfig](
    localAddr: NodeAddress,
    seedNodes: List[NodeAddress],
    shuffleSchedule: Schedule[R, ViewState, Any]
  ): ZLayer[R, Error, PeerService] = {
    type R1 = R with Views
    val layer = ZLayer.identity[R] ++ Views.fromConfig(localAddr)
    layer >>> ZLayer.fromManaged {
      for {
        env <- ZManaged.environment[R1]
        cfg <- getConfig.toManaged_
        _ <- log
              .info(s"Starting HyParView on $localAddr with configuration:\n${cfg.prettyPrint}")
              .toManaged(_ => log.info("Shut down HyParView"))
        scope <- ZManaged.scope
        connections <- Queue
                        .bounded[
                          (NodeAddress, Chunk[Byte] => IO[TransportError, Unit], Stream[Error, Chunk[Byte]], UIO[_])
                        ](
                          cfg.connectionBuffer
                        )
                        .toManaged(_.shutdown)
        plumTreeMessages <- Queue
                             .sliding[Take[Error, (NodeAddress, PlumTreeProtocol)]](cfg.userMessagesBuffer)
                             .toManaged(_.shutdown)
        peerEvents <- Queue.sliding[PeerEvent](128).toManaged(_.shutdown)
        sendInitial0 = (to: NodeAddress, msg: InitialProtocol.InitialMessage) =>
          sendInitial(to, msg, scope, connections).provide(env)
        _ <- receiveInitialProtocol[R1, Error](Transport.bind(localAddr), cfg.concurrentIncomingConnections)
              .merge(ZStream.fromQueue(connections))
              .merge(neighborProtocol.scheduleElements(Schedule.spaced(2.seconds)))
              .flatMapParSwitch(cfg.activeViewCapacity) {
                case (addr, send, receive, release) =>
                  ZStream
                    .fromEffect(peerEvents.offer(PeerEvent.NeighborUp(addr)))
                    .ensuring(peerEvents.offer(PeerEvent.NeighborDown(addr)))
                    .flatMap(_ => runActiveProtocol[R1, Error](addr, send, sendInitial0)(receive).ensuring(release))
              }
              .into(plumTreeMessages)
              .toManaged_
              .fork
        _ <- periodic.doShuffle
              .repeat(shuffleSchedule)
              .toManaged_
              .fork
        _ <- periodic.doReport
              .repeat(Schedule.spaced(2.seconds))
              .toManaged_
              .fork
        _ <- ZIO.foreach_(seedNodes)(sendInitial0(_, InitialProtocol.Join(localAddr))).toManaged_
      } yield new PeerService.Service {
        override val identity: ZIO[Any, Nothing, NodeAddress] =
          ZIO.succeed(localAddr)

        override val getPeers: IO[Nothing, Set[NodeAddress]] =
          Views.activeView.commit.provide(env)

        override def send(to: NodeAddress, message: PlumTreeProtocol): IO[SendError, Unit] =
          Views.send(to, message).provide(env)

        override val receive: ZStream[Any, Error, (NodeAddress, PlumTreeProtocol)] =
          ZStream.fromQueue(plumTreeMessages).unTake

        override val events: ZStream[Any, Nothing, PeerEvent] =
          ZStream.fromQueue(peerEvents)
      }
    }
  }
}
