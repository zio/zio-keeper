package zio.keeper.hyparview

import zio.keeper.hyparview.ActiveProtocol.PlumTreeProtocol
import zio.keeper.{ Error, NodeAddress, SendError }
import zio.stream.{ Stream, ZStream }
import zio.{ IO, UIO, ZIO }

object PeerService {

  def identity: ZIO[PeerService, Nothing, NodeAddress] =
    ZIO.accessM(_.get.identity)

  def getPeers: ZIO[PeerService, Nothing, Set[NodeAddress]] =
    ZIO.accessM(_.get.getPeers)

  def send(to: NodeAddress, message: PlumTreeProtocol): ZIO[PeerService, SendError, Unit] =
    ZIO.accessM(_.get.send(to, message))

  def receive: ZStream[PeerService, Error, (NodeAddress, PlumTreeProtocol)] =
    ZStream.accessStream(_.get.receive)

  def events: ZStream[PeerService, Nothing, PeerEvent] =
    ZStream.accessStream(_.get.events)

  trait Service {
    val identity: UIO[NodeAddress]

    val getPeers: UIO[Set[NodeAddress]]

    def send(to: NodeAddress, message: PlumTreeProtocol): IO[SendError, Unit]

    val receive: Stream[Error, (NodeAddress, PlumTreeProtocol)]

    val events: Stream[Nothing, PeerEvent]
  }

}
