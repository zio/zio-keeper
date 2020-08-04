package zio.keeper.hyparview

import zio._
import zio.stm._
import zio.keeper._
import zio.logging.{Logging, log}

object protocols {

  val initialProtocol: Protocol[HyParViewConfig with Views with Logging, Error, Message, Message, Option[NodeAddress]] =
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
          } yield (Chunk.single(Message.JoinReply(localAddr)), Left(Some(sender)))
        case Message.Neighbor(sender, isHighPriority) =>
          val accept =
            (Chunk.single(Message.NeighborAccept), Left(Some(sender)))
          val reject =
            (Chunk.single(Message.NeighborReject), Left(None))
          if (isHighPriority) {
            ZIO.succeedNow(accept)
          } else {
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
            .as((Chunk.empty, Left(None)))
        case Message.ForwardJoinReply(sender) =>
          ZIO.succeedNow((Chunk.empty, Left(Some(sender))))
        case msg =>
          log.warn(s"Unsupported message for initial protocol: $msg").as((Chunk.empty, Right(initialProtocol)))
      }

}
