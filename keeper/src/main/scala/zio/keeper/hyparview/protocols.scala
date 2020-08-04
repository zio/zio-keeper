package zio.keeper.hyparview

import zio._
import zio.stm._
import zio.keeper._
import zio.logging.{ Logging, log }
import zio.stream.ZStream
import zio.keeper.transport.Connection

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

  def makeActiveProtocol[R <: Views, E](
    con: Connection[R, E, Message, Message]
  ): ZManaged[R, Nothing, NodeAddress => URIO[R, Protocol[R, E, Any, Nothing, Unit]]] =
    ZManaged.switchable[R, Nothing, Protocol[R, E, Any, Nothing, Unit]].flatMap { allocate =>
      Ref.makeManaged(false).flatMap { keepRef =>
        TQueue.bounded[Either[TPromise[Nothing, Unit], Message]](256).commit.toManaged_.map {
          queue => (remoteAddress: NodeAddress) =>
            {
              val disconnect = for {
                myself <- Views.myself.commit
                keep   <- keepRef.get
                _ <- ZSTM.atomically {
                      Views.removeFromActiveView(remoteAddress) *> {
                        if (keep) Views.addToPassiveView(remoteAddress) else STM.unit
                      }
                    }
                _ <- con.send(Message.Disconnect(myself, keep))
              } yield ()
              val handleMessages = ZStream
                .fromTQueue(queue)
                .foreachWhile(_.fold(prom => disconnect *> prom.succeed(()).commit.as(false), con.send(_).as(true)))
              handleMessages.fork *> allocate {
                val signalDisconnect =
                  TPromise.make[Nothing, Unit].flatMap(prom => queue.offer(Left(prom)) *> prom.await)
                ZManaged
                  .make(Views.addToActiveView0(remoteAddress, msg => queue.offer(Right(msg)), signalDisconnect).commit)(
                    _ => signalDisconnect.commit
                  )
                  .as(activeProtocol(remoteAddress))
              } <* handleMessages.fork
            }
        }
      }
    }

  def activeProtocol(remoteAddress: NodeAddress): Protocol[Any, Nothing, Any, Nothing, Unit] = ???

}
