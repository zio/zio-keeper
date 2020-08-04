package zio.keeper.hyparview

import zio._
import zio.stm._
import zio.keeper._
import zio.logging.{ Logging, log }
import zio.stream.ZStream
import zio.keeper.transport.Connection

object protocols {

  def run[R <: HyParViewConfig with Views with Logging](
    con: Connection[R, Nothing, Message, Message]
  ): ZIO[R, Error, Unit] =
    withActiveProtocol(con) { activeProtocol =>
      val protocol =
        initialProtocol
          .contM[R, Error, Message, Message, Any](
            _.fold[ZIO[R, Error, Option[Protocol[R, Error, Message, Message, Unit]]]](ZIO.succeedNow(None)) {
              remoteAddr =>
                activeProtocol(remoteAddr).map(protocol => Some(protocol.unit))
            }
          )
      Protocol.run(con, protocol).unit
    }

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

  // provided function may only be called once. Resources will leak otherwise!
  def withActiveProtocol[R <: Views, A](
    con: Connection[R, Nothing, Message, Message]
  )(
    f: (NodeAddress => URIO[R, Protocol[R, Error, Any, Nothing, Boolean]]) => ZIO[R, Error, A]
  ): ZIO[R, Error, A] =
    TPromise
      .make[Nothing, Unit]
      .commit
      .toManaged_
      .flatMap { disconnected =>
        TRef.make[Option[NodeAddress]](None).commit.toManaged_.flatMap { remoteAddressRef =>
          val awaitDone = remoteAddressRef.get.flatMap[Any, Nothing, Unit](_.fold(STM.unit)(_ => disconnected.await))
          Ref.makeManaged(false).flatMap { keepRef =>
            val disconnect = for {
              myself        <- Views.myself.commit
              keep          <- keepRef.get
              remoteAddress <- remoteAddressRef.get.commit
              _ <- remoteAddress.fold[ZIO[R, Nothing, Unit]](ZIO.unit) { remoteAddress =>
                    val complete =
                      ZSTM.ifM(disconnected.succeed(()))(
                        Views.removeFromActiveView(remoteAddress) *> {
                          if (keep) Views.addToPassiveView(remoteAddress) else STM.unit
                        }.as(true),
                        STM.succeed(false)
                      )
                    complete.commit.flatMap {
                      case true =>
                        con.send(Message.Disconnect(myself, keep))
                      case false =>
                        ZIO.unit
                    }
                  }
            } yield ()
            TQueue.bounded[Option[Message]](256).commit.toManaged_.flatMap { queue =>
              val signalDisconnect = queue.offer(None) *> awaitDone
              ZStream
                .fromTQueue(queue)
                .foldM(true) {
                  case (true, Some(message)) =>
                    con.send(message).as(true)
                  case (true, None) =>
                    disconnect.as(false)
                  case _ =>
                    ZIO.succeedNow(false)
                }
                .toManaged_
                .fork
                .mapM { _ =>
                  f { (remoteAddress: NodeAddress) =>
                    {
                      val setup = remoteAddressRef.set(Some(remoteAddress)) *> Views
                        .addToActiveView0(
                          remoteAddress,
                          msg => queue.offer(Some(msg)),
                          signalDisconnect
                        )
                      val protocol = activeProtocol(remoteAddress).onEnd(keepRef.set(_))
                      setup.commit.as(protocol)
                    }
                  }.ensuring(signalDisconnect.commit)
                }
            }
          }
        }
      }
      .use(ZIO.succeedNow(_))

  def activeProtocol(remoteAddress: NodeAddress): Protocol[Any, Nothing, Any, Nothing, Boolean] = ???

}
