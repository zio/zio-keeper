package zio.keeper.hyparview

import zio.keeper.NodeAddress
import zio.keeper.transport.Transport
import zio.stream.{ Stream, ZStream }
import zio.{ IO, UIO, ZIO }
import zio.ZLayer
import zio.keeper.ByteCodec
import zio.keeper.hyparview.Message.PeerMessage

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

  def live: ZLayer[HyParViewConfig with Transport with TRandom, Nothing, PeerService] =
    ZLayer.fromManaged {
      for {
        cfg         <- HyParViewConfig.getConfig.toManaged_
        views       = Views.live(cfg.address, cfg.activeViewCapacity, cfg.passiveViewCapacity)
        connections = Transport.bind(cfg.address)
        _ <- connections
              .foreachManaged { rawConnection =>
                val con = rawConnection
                  .biMapM(ByteCodec.encode[Message], ByteCodec.decode[Message])
                protocols.all(con.closeOnError)
              }
              .provideSomeLayer[HyParViewConfig with TRandom with Transport](views)
              .fork
      } yield ???
    }

}
