package zio.keeper.hyparview.plumtree

import zio._
import zio.keeper.NodeAddress
import zio.keeper.hyparview.{ PeerService, TRandom }
import zio.logging.log
import zio.logging.Logging
import zio.stm._

object PeerState {

  trait Service {
    val eagerPushPeers: STM[Nothing, Set[NodeAddress]]
    val lazyPushPeers: STM[Nothing, Set[NodeAddress]]
    def addToEagerPeers(peer: NodeAddress): STM[Nothing, Unit]
    def moveToLazyPeers(peer: NodeAddress): STM[Nothing, Option[NodeAddress]]
    def removePeer(peer: NodeAddress): STM[Nothing, Unit]
  }

  def eagerPushPeers: ZSTM[PeerState, Nothing, Set[NodeAddress]] =
    ZSTM.accessM(_.get.eagerPushPeers)

  def lazyPushPeers: ZSTM[PeerState, Nothing, Set[NodeAddress]] =
    ZSTM.accessM(_.get.lazyPushPeers)

  def addToEagerPeers(peer: NodeAddress): ZSTM[PeerState, Nothing, Unit] =
    ZSTM.accessM(_.get.addToEagerPeers(peer))

  def moveToLazyPeers(peer: NodeAddress): ZSTM[PeerState, Nothing, Option[NodeAddress]] =
    ZSTM.accessM(_.get.moveToLazyPeers(peer))

  def removePeer(peer: NodeAddress): ZSTM[PeerState, Nothing, Unit] =
    ZSTM.accessM(_.get.removePeer(peer))

  def live[A: Tag](
    initialEagerPeers: Int
  ): ZLayer[TRandom with PeerService with Logging, Nothing, PeerState] =
    ZLayer.fromEffect {
      for {
        activeView <- PeerService.getPeers
        initial    <- TRandom.selectN(activeView.toList, initialEagerPeers).commit
        _          <- log.info(s"Creating PeerState with initial peers ${initial.mkString("[", ", ", "]")}")
        eagerPeers <- TSet.fromIterable(initial).commit
        lazyPeers  <- TSet.empty[NodeAddress].commit
      } yield new Service {

        override val eagerPushPeers: STM[Nothing, Set[NodeAddress]] =
          eagerPeers.toList.map(_.toSet)

        override val lazyPushPeers: STM[Nothing, Set[NodeAddress]] =
          lazyPeers.toList.map(_.toSet)

        override def addToEagerPeers(peer: NodeAddress): STM[Nothing, Unit] =
          lazyPeers.delete(peer) *> eagerPeers.put(peer)

        override def moveToLazyPeers(peer: NodeAddress): STM[Nothing, Option[NodeAddress]] =
          for {
            contains <- eagerPeers.contains(peer)
            result <- if (contains) eagerPeers.delete(peer) *> lazyPeers.put(peer).as(Some(peer))
                     else STM.succeed(None)
          } yield result

        override def removePeer(peer: NodeAddress): STM[Nothing, Unit] =
          eagerPeers.delete(peer) *> lazyPeers.delete(peer)
      }
    }
}
