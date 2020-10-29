package zio.keeper.hyparview

import zio._
import zio.stm._
import zio.keeper._
import zio.stream.ZStream
import zio.keeper.transport.{ Connection, Protocol }
import zio.keeper.transport.Transport

object protocols {

  def runInitial[R <: HyParViewConfig with Views with TRandom, E](
    con: Connection[R, E, Message, Message]
  ): ZIO[R, E, Unit] =
    ZManaged.switchable[R, E, Boolean => UIO[Unit]].use { switch =>
      val protocol =
        initialProtocol
          .contM[R, E, Message, Message, Any] {
            case Some(remoteAddr) =>
              switch(inActiveView(remoteAddr, con.send)).map { setKeepRef =>
                Right(activeProtocol(remoteAddr).onEnd(setKeepRef).unit)
              }
            case None =>
              ZIO.succeedNow(Left(()))
          }
      protocol.run(con).unit
    }

  def runActive[R <: HyParViewConfig with Views with TRandom, E](
    remoteAddress: NodeAddress,
    con: Connection[R, E, Message, Message]
  ): ZIO[R, E, Unit] =
    inActiveView(remoteAddress, con.send).use { setKeepRef =>
      activeProtocol(remoteAddress).onEnd(setKeepRef).run(con).unit
    }

  // start protocol with remote node
  def connectRemote(
    addr: NodeAddress,
    initialMessage: Message
  ): ZIO[Transport with HyParViewConfig with Views with TRandom, Error, Unit] = {
    val initial = Transport.connect(addr).map(_.withCodec[Message]()).use { con =>
      con.send(initialMessage) *> protocols.runInitial(con)
    }
    val active = Transport.connect(addr).map(_.withCodec[Message]()).use { con =>
      con.send(initialMessage) *> protocols.runActive(addr, con)
    }
    val nothing = ZIO.unit

    initialMessage match {
      case _: Message.ShuffleReply =>
        initial
      case Message.Neighbor(_, true) =>
        active
      case Message.Neighbor(_, false) =>
        Transport
          .connect(addr)
          .map(_.withCodec[Message]())
          .use { con =>
            for {
              _     <- con.send(initialMessage)
              reply <- con.receive.runHead
              _ <- reply match {
                    case Some(Message.NeighborAccept) =>
                      protocols.runActive(addr, con)
                    case _ =>
                      Views.doNeighbor.commit
                  }
            } yield ()
          }
          .onError(_ => Views.doNeighbor.commit)
      case _: Message.Join =>
        active
      case _: Message.ForwardJoinReply =>
        active
      case _ =>
        nothing
    }
  }

  val initialProtocol
    : Protocol[HyParViewConfig with Views with TRandom, Nothing, Message, Message, Option[NodeAddress]] =
    Protocol.fromTransaction {
      case Message.Join(sender) =>
        for {
          config <- HyParViewConfig.getConfigSTM
          others <- Views.activeView.map(_.filterNot(_ == sender))
          _ <- ZSTM.foreach(others.toList)(
                node =>
                  Views
                    .send(
                      node,
                      Message
                        .ForwardJoin(sender, TimeToLive(config.arwl))
                    )
              )
        } yield (Chunk.empty, Left(Some(sender)))
      case Message.Neighbor(sender, true) =>
        STM.succeedNow((Chunk.empty, Left(Some(sender))))
      case Message.Neighbor(sender, false) =>
        val accept =
          (Chunk.single(Message.NeighborAccept), Left(Some(sender)))
        val reject =
          (Chunk.single(Message.NeighborReject), Left(None))
        ZSTM
          .ifM(Views.isActiveViewFull)(
            Views.addToPassiveView(sender).as(reject),
            STM.succeedNow(accept)
          )
      case Message.ShuffleReply(passiveNodes, sentOriginally) =>
        Views.addShuffledNodes(sentOriginally.toSet, passiveNodes.toSet).as((Chunk.empty, Left(None)))
      case Message.ForwardJoinReply(sender) =>
        STM.succeedNow((Chunk.empty, Left(Some(sender))))
      case _ =>
        STM.succeedNow((Chunk.empty, Left(None)))
    }

  def activeProtocol(
    remoteAddress: NodeAddress
  ): Protocol[Views with HyParViewConfig with TRandom, Nothing, Message, Message, Boolean] = {
    lazy val protocol: Protocol[Views with HyParViewConfig with TRandom, Nothing, Message, Message, Boolean] =
      Protocol.fromTransaction {
        case Message.Disconnect(keep) =>
          STM.succeed((Chunk.empty, Left(keep)))
        case msg: Message.Shuffle =>
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
                  _ <- Views.send(msg.originalSender, Message.ShuffleReply(replyNodes, sentNodes))
                } yield ()
              case (_, Some(ttl)) =>
                for {
                  target <- Views.activeView
                             .map(_.filterNot(n => n == msg.sender || n == msg.originalSender).toList)
                             .flatMap(TRandom.selectOne)
                  localAddr <- Views.myself
                  forward   = msg.copy(sender = localAddr, ttl = ttl)
                  _         <- target.fold[ZSTM[Views, Nothing, Unit]](STM.unit)(Views.send(_, forward))
                } yield ()
            }
            .as((Chunk.empty, Right(protocol)))
        case Message.ShuffleReply(passiveNodes, sentOriginally) =>
          Views
            .addShuffledNodes(sentOriginally.toSet, passiveNodes.toSet)
            .as((Chunk.empty, Right(protocol)))
        case Message.ForwardJoin(originalSender, ttl) =>
          HyParViewConfig.getConfigSTM
            .flatMap { config =>
              val accept = for {
                myself <- Views.myself
                _      <- Views.send(originalSender, Message.ForwardJoinReply(myself))
              } yield ()
              Views.activeView.map(av => (av.filter(_ != remoteAddress), ttl.step)).flatMap {
                case (_, None) =>
                  accept
                case (activeView, _) if activeView.size == 0 =>
                  accept
                case (activeView, Some(newTtl)) =>
                  for {
                    _        <- Views.addToPassiveView(originalSender).when(newTtl.count == config.prwl)
                    nextNode <- TRandom.selectOne(activeView.toList)
                    _ <- nextNode.fold[ZSTM[Views, Nothing, Unit]](STM.unit)(
                          Views.send(_, Message.ForwardJoin(originalSender, newTtl))
                        )
                  } yield ()
              }
            }
            .as((Chunk.empty, Right(protocol)))
        case msg: Message.PeerMessage =>
          Views.peerMessage(remoteAddress, msg).as((Chunk.empty, Right(protocol)))
        case _ =>
          STM.succeedNow((Chunk.empty, Right(protocol)))
      }
    protocol
  }

  /**
   * Add the node to active view, using the send function to send messages to the node.
   * The returned function allows to control whether the node will be kept in the passive view
   * on being removed from the active view.
   *
   * This is an exhaustive list of the behavior:
   * - if the ZManaged scope is exited without ever being called, a `Disconnect(false)` is sent and
   *   the node will not be kept in the passive view.
   * - if the ZManaged scope is exited after the function is called with `b`, no disconnect
   *   message will be sent and the node is kept in the passive view if `b` is true.
   * - if the node is removed from the active view using `Views#removeFromActiveView` a disconnect
   *   message will be sent to the node.
   *
   * At most one disconnect message will ever be sent.
   */
  private def inActiveView[R <: Views](
    remoteAddress: NodeAddress,
    send: Message => ZIO[R, Any, Unit]
  ): ZManaged[R, Nothing, Boolean => UIO[Unit]] =
    for {
      disconnected <- Promise.make[Nothing, Unit].toManaged_
      keepRef      <- TRef.make[Option[Boolean]](None).commit.toManaged_
      queue        <- TQueue.bounded[Message](256).commit.toManaged_
      _ <- ZStream
            .fromTQueue(queue)
            .foldManagedM(true) {
              case (true, msg) =>
                keepRef
                  .modify {
                    (_, msg) match {
                      case (k @ Some(_), _) =>
                        (disconnected.succeed(()).as(false), k)
                      case (None, msg @ Message.Disconnect(b)) =>
                        (send(msg) *> disconnected.succeed(()).as(false), Some(b))
                      case (None, msg) =>
                        (send(msg).as(true), None)
                    }
                  }
                  .commit
                  .flatten
              case _ =>
                ZIO.succeedNow(false)
            }
            .ensuring(disconnected.succeed(()))
            .fork
      _ <- Views
            .addToActiveView(remoteAddress, queue.offer, b => queue.offer(Message.Disconnect(b)))
            .commit
            .toManaged { _ =>
              keepRef.get.flatMap { k =>
                Views.removeFromActiveView(remoteAddress, k.getOrElse(false))
              }.commit *> disconnected.await
            }
    } yield (b: Boolean) => keepRef.update(_.orElse(Some(b))).commit

}
