package zio.keeper.hyparview

import zio._
import zio.stm._
import zio.keeper._
import zio.logging.{ Logging, log }
import zio.stream.ZStream
import zio.keeper.transport.{ Connection, Protocol }

object protocols {

  def run[R <: HyParViewConfig with Views with Logging with TRandom](
    con: Connection[R, Nothing, Message, Message]
  ): ZIO[R, Error, Unit] =
    withActiveProtocol(con) { activeProtocol =>
      val protocol =
        initialProtocol
          .contM[R, Error, Message, Message, Any] {
            case Some(remoteAddr) =>
              activeProtocol(remoteAddr).map(protocol => Right(protocol.unit))
            case None =>
              ZIO.succeedNow(Left(()))
          }
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
        log.warn(s"Unsupported message for initial protocol: $msg").as((Chunk.empty, Left(None)))
    }

  // provided function may only be called once. Resources will leak otherwise!
  def withActiveProtocol[R <: Views with HyParViewConfig with TRandom, A](
    con: Connection[R, Nothing, Message, Message]
  )(
    f: (NodeAddress => URIO[R, Protocol[R, Error, Message, Message, Boolean]]) => ZIO[R, Error, A]
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
                    con.send(Message.Disconnect(keep)).whenM(complete.commit)
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

  def activeProtocol(
    remoteAddress: NodeAddress
  ): Protocol[Views with HyParViewConfig with TRandom, Nothing, Message, Message, Boolean] =
    Protocol.fromEffect {
      case Message.Disconnect(keep) =>
        ZIO.succeedNow((Chunk.empty, Left(keep)))
      case msg: Message.Shuffle =>
        def sendInitial(to: NodeAddress, message: Message): UIO[Unit] = ???

        Views.activeViewSize
          .map[(Int, Option[TimeToLive])]((_, msg.ttl.step))
          .flatMap {
            case (0, _) | (_, None) =>
              for {
                config    <- HyParViewConfig.getConfigSTM
                passive   <- Views.passiveView
                sentNodes = msg.activeNodes ++ msg.passiveNodes
                replyNodes <- TRandom.selectN(
                               passive.filterNot(_ == msg.originalSender).toList,
                               config.shuffleNActive + config.shuffleNPassive
                             )
                _ <- Views.addAllToPassiveView(sentNodes)
              } yield sendInitial(
                msg.originalSender,
                Message.ShuffleReply(replyNodes, sentNodes)
              )
            case (_, Some(ttl)) =>
              for {
                active    <- Views.activeView.map(_.filterNot(n => n == msg.sender || n == msg.originalSender).toList)
                localAddr <- Views.myself
                forward   = msg.copy(sender = localAddr, ttl = ttl)
              } yield {
                def go(candidates: List[NodeAddress]): ZSTM[TRandom with Views, Nothing, Unit] =
                  TRandom
                    .selectOne(candidates)
                    .flatMap(
                      _.fold[ZSTM[TRandom with Views, Nothing, Unit]](STM.unit)(
                        c => Views.send0(c, forward).orElse(go(candidates.filterNot(_ == c)))
                      )
                    )
                go(active).commit
              }
          }
          .commit
          .flatten
          .as((Chunk.empty, Right(activeProtocol(remoteAddress))))
      case Message.ShuffleReply(passiveNodes, sentOriginally) =>
        Views
          .addShuffledNodes(sentOriginally.toSet, passiveNodes.toSet)
          .commit
          .as((Chunk.empty, Right(activeProtocol(remoteAddress))))
      case _ =>
        ZIO.succeedNow((Chunk.empty, Right(activeProtocol(remoteAddress))))
    }

}
