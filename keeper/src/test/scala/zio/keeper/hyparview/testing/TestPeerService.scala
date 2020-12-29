package zio.keeper.hyparview.testing

import zio._
import zio.stm._
import zio.stream._
import zio.keeper.NodeAddress
import zio.keeper.hyparview.Message.PeerMessage
import zio.keeper.hyparview.{ PeerEvent, PeerService }
import zio.keeper.hyparview.PeerEvent._

object TestPeerService {

  trait Service {
    def setPeers(peers: Set[NodeAddress]): USTM[Unit]
    def offerMessage(sender: NodeAddress, msg: PeerMessage): USTM[Unit]
    val outgoingMessages: Stream[Nothing, (NodeAddress, PeerMessage)]
    def withPeerService[R <: Has[_], E, A](zio: ZIO[R with PeerService, E, A]): ZIO[R, E, A]
  }

  def setPeers(peers: Set[NodeAddress]): ZSTM[TestPeerService, Nothing, Unit] =
    ZSTM.accessM(_.get.setPeers(peers))

  def offerMessage(sender: NodeAddress, msg: PeerMessage): ZSTM[TestPeerService, Nothing, Unit] =
    ZSTM.accessM(_.get.offerMessage(sender, msg))

  val outgoingMessages: ZStream[TestPeerService, Nothing, (NodeAddress, PeerMessage)] =
    ZStream.accessStream(_.get.outgoingMessages)

  val peerServiceLayer: ZLayer[TestPeerService, Nothing, PeerService] =
    ZLayer.fromEffect(
      ZIO.accessM(
        _.get.withPeerService[TestPeerService, Nothing, PeerService.Service](ZIO.service[PeerService.Service])
      )
    )

  val make: ZLayer[Any, Nothing, TestPeerService] = ZLayer.fromManaged {
    for {
      ref         <- TRef.make(Set.empty[NodeAddress]).commit.toManaged_
      eventsQueue <- TQueue.unbounded[PeerEvent].commit.toManaged_
      outgoing    <- TQueue.unbounded[(NodeAddress, PeerMessage)].commit.toManaged_
    } yield new Service {
      override def setPeers(peers: Set[NodeAddress]): USTM[Unit] =
        for {
          oldPeers <- ref.modify((_, peers))
          _        <- eventsQueue.offerAll(oldPeers.diff(peers).map(NeighborDown.apply).toList)
          _        <- eventsQueue.offerAll(peers.diff(oldPeers).map(NeighborUp.apply).toList)
        } yield ()

      override def offerMessage(sender: NodeAddress, msg: PeerMessage): USTM[Unit] =
        eventsQueue.offer(MessageReceived(sender, msg))

      override val outgoingMessages: Stream[Nothing, (NodeAddress, PeerMessage)] =
        ZStream.fromTQueue(outgoing)

      override def withPeerService[R <: Has[_], E, A](zio: ZIO[R with PeerService, E, A]): ZIO[R, E, A] = {
        val peerService = new PeerService.Service {

          val getPeers: UIO[Set[NodeAddress]] =
            ref.get.commit

          def send(to: NodeAddress, message: PeerMessage): IO[Nothing, Unit] =
            STM.atomically {
              for {
                peers <- ref.get
                _     <- outgoing.offer(to -> message).when(peers.contains(to))
              } yield ()
            }

          val events: Stream[Nothing, PeerEvent] =
            ZStream.fromTQueue(eventsQueue)

        }
        zio.provideSomeLayer[R](ZLayer.succeed(peerService))
      }
    }
  }
}
