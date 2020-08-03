package zio.keeper.hyparview

import zio._
import zio.stm._
import zio.keeper._
import zio.logging.{Logging, log}

object protocols {

  def initialProtocol[R <: HyParViewConfig with Views with Logging, E >: Error, I <: Message, O >: Message](
    cont: NodeAddress => ZIO[R, E, Protocol[R, E, I, O]]
  ): Protocol[R, E, I, O] =
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
            protocol <- cont(sender)
          } yield (Chunk.single(Message.JoinReply(localAddr)), Some(protocol))
        case Message.Neighbor(sender, isHighPriority) =>
          val accept =
            cont(sender).map(protocol => (Chunk.single(Message.NeighborAccept), Some(protocol)))
          val reject =
            ZIO.succeedNow((Chunk.single(Message.NeighborReject), None))
          if (isHighPriority) accept
          else {
            ZSTM
              .ifM(Views.isActiveViewFull)(
                Views.addToPassiveView(sender).as(reject),
                STM.succeedNow(accept)
              )
              .commit
              .flatten
          }
        case Message.ShuffleReply(passiveNodes, sentOriginally) =>
          Views
            .addShuffledNodes(sentOriginally.toSet, passiveNodes.toSet)
            .commit
            .as((Chunk.empty, None))
        case Message.ForwardJoinReply(sender) =>
          cont(sender).map(protocol => (Chunk.empty, Some(protocol)))
        case msg =>
          log.warn(s"Unsupported message for initial protocol: $msg").as((Chunk.empty, Some(go)))
      }

  val makeActiveProtocol: Protocol[Any, Nothing, Any, Nothing] = ???

}
