package zio.keeper.hyparview

import zio._
import zio.stm._
import zio.keeper._
import zio.stream.ZStream
import zio.keeper.transport.{ Connection, Protocol }

object protocols {

  def all[R <: HyParViewConfig with Views with TRandom](
    con: Connection[R, Nothing, Message, Message]
  ): ZIO[R, Unit, Unit] =
    ZManaged.switchable[R, Unit, Boolean => UIO[Unit]].use { switch =>
      val protocol =
        initialProtocol
          .contM[R, Unit, Message, Message, Any] {
            case Some(remoteAddr) =>
              switch(inActiveView(remoteAddr, con.send)).map { setKeepRef =>
                Right(activeProtocol(remoteAddr).onEnd(setKeepRef).unit)
              }
            case None =>
              ZIO.succeedNow(Left(()))
          }
      Protocol.run(con, protocol).unit
    }

  val initialProtocol
    : Protocol[HyParViewConfig with Views with TRandom, Nothing, Message, Message, Option[NodeAddress]] =
    Protocol.fromTransaction {
      case Message.Join(sender) =>
        for {
          config    <- HyParViewConfig.getConfigSTM
          others    <- Views.activeView.map(_.filterNot(_ == sender))
          localAddr <- Views.myself
          _ <- ZSTM.foreach(others)(
                node =>
                  Views
                    .send(
                      node,
                      Message
                        .ForwardJoin(sender, TimeToLive(config.arwl))
                    )
              )
        } yield (Chunk.single(Message.JoinReply(localAddr)), Left(Some(sender)))
      case Message.Neighbor(sender, isHighPriority) =>
        val accept =
          (Chunk.single(Message.NeighborAccept), Left(Some(sender)))
        val reject =
          (Chunk.single(Message.NeighborReject), Left(None))
        if (isHighPriority) {
          STM.succeedNow(accept)
        } else {
          ZSTM
            .ifM(Views.isActiveViewFull)(
              Views.addToPassiveView(sender).as(reject),
              STM.succeedNow(accept)
            )
        }
      case Message.ShuffleReply(passiveNodes, sentOriginally) =>
        addShuffledNodes(sentOriginally.toSet, passiveNodes.toSet).as((Chunk.empty, Left(None)))
      case Message.ForwardJoinReply(sender) =>
        STM.succeedNow((Chunk.empty, Left(Some(sender))))
      case _ =>
        STM.succeedNow((Chunk.empty, Left(None)))
    }

  def activeProtocol(
    remoteAddress: NodeAddress
  ): Protocol[Views with HyParViewConfig with TRandom, Nothing, Message, Message, Boolean] =
    Protocol.fromTransaction {
      case Message.Disconnect(keep) =>
        STM.succeedNow((Chunk.empty, Left(keep)))
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
          .as((Chunk.empty, Right(activeProtocol(remoteAddress))))
      case Message.ShuffleReply(passiveNodes, sentOriginally) =>
        addShuffledNodes(sentOriginally.toSet, passiveNodes.toSet)
          .as((Chunk.empty, Right(activeProtocol(remoteAddress))))
      case Message.ForwardJoin(originalSender, ttl) =>
        HyParViewConfig.getConfigSTM
          .flatMap { config =>
            Views.activeView.map(av => (av.filter(_ != remoteAddress), ttl.step)).flatMap {
              case (_, None) =>
                STM.unit // TODO: connect
              case (activeView, _) if activeView.size == 0 =>
                STM.unit // TODO: connect
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
          .as((Chunk.empty, Right(activeProtocol(remoteAddress))))
      case _ =>
        STM.succeedNow((Chunk.empty, Right(activeProtocol(remoteAddress))))
    }

  def inActiveView[R <: Views](
    remoteAddress: NodeAddress,
    send: Message => ZIO[R, Nothing, Unit]
  ): ZManaged[R, Unit, Boolean => UIO[Unit]] =
    for {
      env          <- ZManaged.environment[R]
      disconnected <- Promise.make[Nothing, Unit].toManaged_
      keepRef      <- TRef.make(false).commit.toManaged_
      queue        <- TQueue.bounded[Either[Boolean, Message]](256).commit.toManaged_
      _ <- ZStream
            .fromTQueue(queue)
            .foldManagedM(true) {
              case (true, Right(message)) =>
                send(message).as(true)
              case (true, Left(keep)) =>
                (send(Message.Disconnect(keep)) *> disconnected.succeed(())).as(false)
              case _ =>
                ZIO.succeedNow(false)
            }
            .ensuring(disconnected.succeed(()))
            .fork
      signalDisconnect = for {
        keep <- keepRef.get
        _    <- Views.addToPassiveView(remoteAddress).when(keep)
        _    <- queue.offer(Left(keep))
      } yield ()

      _ <- Views
            .addToActiveView(remoteAddress, msg => queue.offer(Right(msg)), signalDisconnect.provide(env))
            .commit
            .toManaged(_ => Views.removeFromActiveView(remoteAddress).commit *> disconnected.await)
    } yield (b: Boolean) => keepRef.set(b).commit

  def addShuffledNodes(
    sentOriginally: Set[NodeAddress],
    replied: Set[NodeAddress]
  ): ZSTM[Views with TRandom, Nothing, Unit] = {
    val dropOneFromPassive: ZSTM[Views with TRandom, Nothing, Unit] =
      for {
        list    <- Views.passiveView.map(_.toList)
        dropped <- TRandom.selectOne(list)
        _       <- ZSTM.foreach_(dropped)(Views.removeFromPassiveView)
      } yield ()

    def dropNFromPassive(n: Int): ZSTM[Views with TRandom, Nothing, Unit] =
      if (n <= 0) STM.unit else (dropOneFromPassive *> dropNFromPassive(n - 1))

    for {
      _         <- ZSTM.foreach(sentOriginally)(Views.removeFromPassiveView)
      size      <- Views.passiveViewSize
      capacity  <- Views.passiveViewCapacity
      _         <- dropNFromPassive(replied.size - (capacity - size))
      _         <- Views.addAllToPassiveView(replied.toList)
      remaining <- Views.passiveViewSize.map(capacity - _)
      _         <- Views.addAllToPassiveView(sentOriginally.take(remaining).toList)
    } yield ()
  }

}
