package zio.keeper.hyparview

import zio.keeper.NodeAddress
import zio.keeper.transport.Transport
import zio.stream.{ Stream, ZStream }
import zio._
import zio.clock.Clock
import zio.duration._
import zio.ZLayer
import zio.logging.Logging
import zio.keeper.hyparview.Message.PeerMessage
import zio.keeper.hyparview.ViewEvent._

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
    reportInterval: Duration = 2.seconds,
    messagesBuffer: Int = 128
  ): ZLayer[R, Nothing, PeerService] =
    ZLayer.fromManaged {

      for {
        cfg         <- HyParViewConfig.getConfig.toManaged_
        connections = Transport.bind(cfg.address)
        peerEventsQ <- Queue.sliding[PeerEvent](messagesBuffer).toManaged_
        viewsLayer  = Views.live
        env         <- ZManaged.environment[R with Views].provideSomeLayer[R](viewsLayer)
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
            _ <- connections
                  .map {
                    _.use { con =>
                      protocols.runInitial(con.withCodec[Message]())
                    }
                  }
                  .merge {
                    Views.events.flatMap {
                      case AddedToActiveView(node) =>
                        ZStream.fromEffect(peerEventsQ.offer(PeerEvent.NeighborUp(node))).drain
                      case PeerMessageReceived(node, msg) =>
                        ZStream.fromEffect(peerEventsQ.offer(PeerEvent.MessageReceived(node, msg))).drain
                      case RemovedFromActiveView(node) =>
                        ZStream.fromEffect(peerEventsQ.offer(PeerEvent.NeighborDown(node))).drain
                      case UnhandledMessage(to, msg) =>
                        ZStream.succeed(protocols.connectRemote(to, msg))
                      case _ =>
                        ZStream.empty
                    }
                  }
                  .mapMParUnordered(workers)(
                    _.ignore
                  )
                  .runDrain
                  .toManaged_
                  .fork
          } yield ()
        }.provide(env)
      } yield new PeerService.Service {
        override val getPeers: UIO[Set[NodeAddress]] =
          Views.activeView.commit.provide(env)
        override def send(to: NodeAddress, message: PeerMessage): IO[Nothing, Unit] =
          Views.send(to, message).commit.provide(env)
        override val events: Stream[Nothing, PeerEvent] =
          ZStream.fromQueue(peerEventsQ)
      }
    }

}
