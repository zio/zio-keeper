package zio.keeper.hyparview

import zio.keeper.NodeAddress
import zio.keeper.transport.Transport
import zio.stream.{ Stream, ZStream }
import zio.{ IO, Schedule, UIO, ZIO }
import zio.clock.Clock
import zio.duration._
import zio.ZLayer
import zio.keeper.hyparview.Message.PeerMessage
import zio.logging.Logging

object PeerService {

  trait Service {
    val identity: UIO[NodeAddress]
    val getPeers: UIO[Set[NodeAddress]]
    def send(to: NodeAddress, message: PeerMessage): IO[Nothing, Unit]
    val receive: Stream[Nothing, (NodeAddress, PeerMessage)]
    val events: Stream[Nothing, PeerEvent]
  }

  def identity: ZIO[PeerService, Nothing, NodeAddress] =
    ZIO.accessM(_.get.identity)

  def getPeers: ZIO[PeerService, Nothing, Set[NodeAddress]] =
    ZIO.accessM(_.get.getPeers)

  def send(to: NodeAddress, message: PeerMessage): ZIO[PeerService, Nothing, Unit] =
    ZIO.accessM(_.get.send(to, message))

  def receive: ZStream[PeerService, Nothing, (NodeAddress, PeerMessage)] =
    ZStream.accessStream(_.get.receive)

  def events: ZStream[PeerService, Nothing, PeerEvent] =
    ZStream.accessStream(_.get.events)

  def live[R <: HyParViewConfig with Transport with TRandom with Logging with Clock](
    shuffleSchedule: Schedule[R, ViewState, Any],
    fibersForIncomingConnections: Int = 32,
    reportInterval: Duration = 2.seconds
  ): ZLayer[R, Nothing, PeerService] =
    ZLayer.fromManaged {
      for {
        cfg         <- HyParViewConfig.getConfig.toManaged_
        views       = Views.live(cfg.address, cfg.activeViewCapacity, cfg.passiveViewCapacity)
        connections = Transport.bind(cfg.address)
        _ <- {
          for {
            _ <- connections
                  .mapMParUnordered(fibersForIncomingConnections) { rawConnection =>
                    protocols.all(rawConnection.withCodec[Message]().closeOnError)
                  }
                  .runDrain
                  .toManaged_
                  .fork
            _ <- periodic.doShuffle
                  .repeat(shuffleSchedule)
                  .toManaged_
                  .fork
            _ <- periodic.doReport
                  .repeat(Schedule.spaced(reportInterval))
                  .toManaged_
                  .fork
          } yield ()
        }.provideSomeLayer[R](views)
      } yield ???
    }

}
