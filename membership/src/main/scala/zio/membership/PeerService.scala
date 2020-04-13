package zio.membership

import zio._
import zio.membership.hyparview.ActiveProtocol.PlumTreeProtocol
import zio.stream._

object PeerService {

  def identity[T: Tagged]: ZIO[PeerService[T], Nothing, T] =
    ZIO.accessM(_.get.identity)

  def getPeers[T: Tagged]: ZIO[PeerService[T], Nothing, Set[T]] =
    ZIO.accessM(_.get.getPeers)

  def send[T: Tagged](to: T, message: PlumTreeProtocol): ZIO[PeerService[T], SendError, Unit] =
    ZIO.accessM(_.get.send(to, message))

  def receive[T: Tagged]: ZStream[PeerService[T], Error, (T, PlumTreeProtocol)] =
    ZStream.accessStream(_.get.receive)

  def events[T: Tagged]: ZStream[PeerService[T], Nothing, PeerEvent[T]] =
    ZStream.accessStream(_.get.events)

  trait Service[T] {
    val identity: UIO[T]

    val getPeers: UIO[Set[T]]

    def send(to: T, message: PlumTreeProtocol): IO[SendError, Unit]

    val receive: Stream[Error, (T, PlumTreeProtocol)]

    val events: Stream[Nothing, PeerEvent[T]]
  }
}
