package zio.keeper.hyparview

import zio._
import zio.stm._
import zio.keeper._

object protocols {

  def initialProtocol: Protocol[HyParViewConfig with Views, Error, Message, Message] =
    Protocol.fromEffect {
      case Message.Join(sender) =>
        for {
          others    <- Views.activeView.map(_.filterNot(_ == sender)).commit
          localAddr <- Views.myself.commit
          config    <- getConfig
          _ <- ZIO
                .foreachPar_(others)(
                  node =>
                    Views
                      .send(
                        node,
                        ActiveProtocol
                          .ForwardJoin(localAddr, sender, TimeToLive(config.arwl))
                      )
                )
        } yield (Chunk.single(Message.JoinReply(localAddr)), Some(activeProtocol(sender)))
      case Message.Neighbor(sender, isHighPriority) =>
        val accept =
          (Chunk.single(Message.NeighborAccept), Some(activeProtocol(sender)))

        val reject =
          (Chunk.single(Message.NeighborReject), None)

        if (isHighPriority) ZIO.succeedNow(accept)
        else {
          ZSTM
            .ifM(Views.isActiveViewFull)(
              Views.addToPassiveView(sender).as(reject),
              STM.succeedNow(accept)
            )
            .commit
        }
      case Message.ShuffleReply(passiveNodes, sentOriginally) =>
        Views
          .addShuffledNodes(sentOriginally.toSet, passiveNodes.toSet)
          .commit
          .as((Chunk.empty, None))
      case Message.ForwardJoinReply(sender) =>
        ZIO.succeedNow((Chunk.empty, Some(activeProtocol(sender))))
      case msg =>
        ZIO.fail(ProtocolError(s"illegal message for initial protocol: ${msg.toString}"))
    }

  def activeProtocol(remoteAddr: NodeAddress): Protocol[Any, Nothing, Any, Nothing] = ???
}
