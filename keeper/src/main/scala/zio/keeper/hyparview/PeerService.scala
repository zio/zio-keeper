package zio.keeper.hyparview

import zio.keeper.NodeAddress
import zio.keeper.transport.Transport
import zio.stream.{ Stream, ZStream }
import zio._
import zio.clock.Clock
import zio.duration._
import zio.ZLayer
import zio.keeper.hyparview.Message.PeerMessage
import zio.logging.Logging
import zio.keeper.hyparview.ViewEvent.UnhandledMessage

object PeerService {

  trait Service {
    val getPeers: UIO[Set[NodeAddress]]
    def send(to: NodeAddress, message: PeerMessage): IO[Nothing, Unit]
    val events: Stream[Nothing, PeerEvent]
  }

  def getPeers: ZIO[PeerService, Nothing, Set[NodeAddress]] =
    ZIO.accessM(_.get.getPeers)

  def send(to: NodeAddress, message: PeerMessage): ZIO[PeerService, Nothing, Unit] =
    ZIO.accessM(_.get.send(to, message))

  def events: ZStream[PeerService, Nothing, PeerEvent] =
    ZStream.accessStream(_.get.events)

  def live[R <: HyParViewConfig with Transport with TRandom with Logging with Clock](
    shuffleSchedule: Schedule[R, ViewState, Any],
    workers: Int = 32,
    reportInterval: Duration = 2.seconds
  ): ZLayer[R, Nothing, PeerService] =
    ZLayer.fromManaged {

      def openConnection(addr: NodeAddress, initialMessages: Chunk[Message]) =
        for {
          rawConnection <- Transport.connect(addr)
          connection    = rawConnection.withCodec[Message]()
          _             <- ZIO.foreach(initialMessages)(connection.send).toManaged_
        } yield connection

      for {
        cfg         <- HyParViewConfig.getConfig.toManaged_
        views       = Views.live(cfg.address, cfg.activeViewCapacity, cfg.passiveViewCapacity)
        connections = Transport.bind(cfg.address)
        _ <- {
          for {
            _ <- periodic.doShuffle
                  .repeat(shuffleSchedule)
                  .toManaged_
                  .fork
            _ <- periodic.doReport
                  .repeat(Schedule.spaced(reportInterval))
                  .toManaged_
                  .fork
            outgoing = Views.events.flatMap {
              case UnhandledMessage(to, msg) =>
                ZStream.managed(openConnection(to, Chunk.single(msg)))
              case _ => ZStream.empty
            }
            incoming = connections.map(_.withCodec[Message]())
            _        <- outgoing.merge(incoming).mapMParUnordered(workers)(protocols.all(_)).runDrain.toManaged_.fork
          } yield ()
        }.provideSomeLayer[R](views)
      } yield ???
    }

}
