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
import zio.keeper.hyparview.ViewEvent.AddedToActiveView
import zio.keeper.hyparview.ViewEvent.RemovedFromActiveView
import zio.keeper.Error

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

      def openConnection(addr: NodeAddress, initialMessages: Chunk[Message]) =
        for {
          rawConnection <- Transport.connect(addr)
          connection    = rawConnection.withCodec[Message]()
          _             <- ZIO.foreach(initialMessages)(connection.send).toManaged_
        } yield connection

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
            outgoing = Views.events.flatMap {
              case UnhandledMessage(to, msg) =>
                ZStream.succeed(openConnection(to, Chunk.single(msg)))
              case AddedToActiveView(node) =>
                ZStream.fromEffect(peerEventsQ.offer(PeerEvent.NeighborUp(node))).drain
              case RemovedFromActiveView(node) =>
                ZStream.fromEffect(peerEventsQ.offer(PeerEvent.NeighborDown(node))).drain
              case _ =>
                ZStream.empty
            }
            incoming = connections.map(_.map(_.withCodec[Message]()))
            _ <- incoming
                  .merge(outgoing)
                  .mapMParUnordered[Views with HyParViewConfig with TRandom with Transport, Error, Unit](workers) {
                    connection =>
                      connection.use(
                        protocols.hyparview(_, peerEventsQ.contramap((PeerEvent.MessageReceived.apply _).tupled))
                      )
                  }
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
