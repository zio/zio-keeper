package zio.membership.hyparview

import zio.membership.transport.ChunkConnection
import zio.membership.Membership
import zio._
import zio.membership.ByteCodec
import zio.membership.ByteCodec._
import zio.membership.transport.Transport
import zio.stm._
import zio.random.Random
import zio.membership.hyparview.Protocol.Disconnect
import zio.membership.Error

object HyParView {

  def apply[T](
    localAddr: T,
    activeViewCapactiy: Int,
    passiveViewCapacity: Int,
    arwl: Int,
    prwl: Int
  ): ZManaged[Transport[T] with Random, Nothing, Membership[T]] = ???

  final case class Config(
    activeViewCapactiy: Int,
    passiveViewCapacity: Int,
    arwl: Int,
    prwl: Int,
    shuffleNActive: Int,
    shuffleNPassive: Int,
    shuffleTTL: Int
  )

  //=========
  // internal
  //=========

  private[hyparview] def startHandler[T](
    con: ChunkConnection
  )(
    implicit
    ev1: ByteCodec[Protocol.Disconnect[T]],
    ev2: ByteCodec[Protocol.ForwardJoin[T]],
    ev3: ByteCodec[Protocol.Shuffle[T]],
    ev4: ByteCodec[Protocol.ShuffleReply[T]],
    ev5: ByteCodec[Protocol.Neighbor[T]],
    ev6: ByteCodec[Protocol.NeighborReject[T]]
  ): ZIO[Env[T] with Transport[T], Nothing, Fiber[Error, Unit]] =
    con.receive.foreach { msg =>
      Protocol.decodeChunk[T](msg).flatMap {
        case m: Protocol.Disconnect[T]     => handleDisconnect(m)
        case m: Protocol.Join[T]           => handleJoin(m).fork
        case m: Protocol.ForwardJoin[T]    => handleForwardJoin(m).fork
        case m: Protocol.Shuffle[T]        => handleShuffle(m)
        case m: Protocol.ShuffleReply[T]   => handleShuffleReply(m)
        case m: Protocol.Neighbor[T]       => handleNeighbor(m).fork
        case m: Protocol.NeighborReject[T] => handleNeighborReject(m).fork
      }
    }.fork

  private[hyparview] def disconnect[T](
    node: T,
    shutDown: Boolean = false
  )(
    implicit
    ev: ByteCodec[Protocol.Disconnect[T]]
  ): ZIO[Transport[T] with Env[T], Nothing, Unit] =
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      (for {
        conOpt <- env.activeView.get(node)
        task <- conOpt match {
                 case Some(con) =>
                   for {
                     _ <- env.activeView.delete(node)
                     _ <- env.addNodeToPassiveView(node)
                   } yield encode(Protocol.Disconnect(env.myself, shutDown)).flatMap(con.send).ignore.unit
                 case None => STM.succeed(ZIO.unit)
               }
      } yield task).commit.flatten
    }

  private[hyparview] def connect[T](
    to: T
  )(
    implicit
    ev1: ByteCodec[Protocol.Disconnect[T]],
    ev2: ByteCodec[Protocol.ForwardJoin[T]],
    ev3: ByteCodec[Protocol.Shuffle[T]],
    ev4: ByteCodec[Protocol.ShuffleReply[T]],
    ev5: ByteCodec[Protocol.Neighbor[T]],
    ev6: ByteCodec[Protocol.NeighborReject[T]]
  ) =
    for {
      con <- ZIO.accessM[Transport[T]](_.transport.connect(to))
      _   <- startHandler(con)
      (myself, highPrio, dropped) <- ZIO.environment[Env[T]].map(_.env).flatMap { env =>
                                      (for {
                                        highPrio <- env.activeView.keys.map(_.isEmpty)
                                        dropped  <- env.addNodeToActiveView(to, con)
                                      } yield (env.myself, highPrio, dropped)).commit
                                    }
      _ <- encode(Protocol.Neighbor(myself, highPrio)).flatMap(con.send)
      _ <- ZIO.foreach(dropped)(disconnect(_))
    } yield ()

  private[hyparview] def send[T, M <: Protocol[T]: ByteCodec](to: T, msg: M) =
    for {
      chunk <- encode(msg)
      con   <- Env.activeView[T].flatMap(_.get(to).commit)
      _     <- ZIO.foreach(con)(_.send(chunk))
    } yield ()

  private[hyparview] def attemptPromotion[T](
    implicit
    ev1: ByteCodec[Protocol.Disconnect[T]],
    ev2: ByteCodec[Protocol.ForwardJoin[T]],
    ev3: ByteCodec[Protocol.Shuffle[T]],
    ev4: ByteCodec[Protocol.ShuffleReply[T]],
    ev5: ByteCodec[Protocol.Neighbor[T]],
    ev6: ByteCodec[Protocol.NeighborReject[T]]
  ) =
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      for {
        promoted <- env.promoteRandom.commit
        _ <- ZIO.foreach(promoted) { node =>
              connect(node).foldM(
                _ => env.passiveView.delete(node).commit,
                _ => ZIO.unit
              )
            }
      } yield ()
    }

  private[hyparview] def sendShuffle[T](
    to: T
  )(
    implicit
    ev: ByteCodec[Protocol.Shuffle[T]]
  ) =
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      for {
        (active, passive) <- (for {
                              active  <- env.activeView.keys.flatMap(env.selectN(_, env.cfg.shuffleNActive))
                              passive <- env.passiveView.toList.flatMap(env.selectN(_, env.cfg.shuffleNPassive))
                            } yield (active, passive)).commit
        _ <- send(to, Protocol.Shuffle(env.myself, env.myself, active, passive, TimeToLive(env.cfg.shuffleTTL)))
      } yield ()
    }

  private[hyparview] def sendOneOf[T, M <: Protocol[T]: ByteCodec](to: T, msg: M) =
    for {
      chunk <- encode(msg)
      _     <- ZIO.accessM[Transport[T]](_.transport.send(to, chunk))
    } yield ()

  private[hyparview] def handleDisconnect[T](
    msg: Protocol.Disconnect[T]
  )(
    implicit ev: ByteCodec[Disconnect[T]]
  ) =
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      STM
        .atomically {
          for {
            inActive <- env.activeView.contains(msg.sender)
            _        <- if (inActive) env.activeView.delete(msg.sender) else STM.unit
            _        <- if (inActive && msg.alive) env.addNodeToPassiveView(msg.sender) else STM.unit
          } yield msg.sender
        }
        .flatMap(disconnect(_))
    }

  private[hyparview] def handleNeighbor[T](
    msg: Protocol.Neighbor[T]
  )(
    implicit
    ev1: ByteCodec[Protocol.Disconnect[T]],
    ev2: ByteCodec[Protocol.ForwardJoin[T]],
    ev3: ByteCodec[Protocol.Shuffle[T]],
    ev4: ByteCodec[Protocol.ShuffleReply[T]],
    ev5: ByteCodec[Protocol.Neighbor[T]],
    ev6: ByteCodec[Protocol.NeighborReject[T]]
  ) =
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      if (msg.isHighPriority) {
        connect(msg.sender)
      } else {
        (for {
          full <- env.isActiveViewFull
          task <- if (full) {
                   env
                     .addNodeToPassiveView(msg.sender)
                     .as(
                       send(msg.sender, Protocol.NeighborReject(env.myself))
                     )
                 } else {
                   STM.succeed(connect(msg.sender))
                 }
        } yield task).commit.flatten
      }
    }

  private[hyparview] def handleJoin[T](
    msg: Protocol.Join[T]
  )(
    implicit
    ev1: ByteCodec[Protocol.Disconnect[T]],
    ev2: ByteCodec[Protocol.ForwardJoin[T]],
    ev3: ByteCodec[Protocol.Shuffle[T]],
    ev4: ByteCodec[Protocol.ShuffleReply[T]],
    ev5: ByteCodec[Protocol.Neighbor[T]],
    ev6: ByteCodec[Protocol.NeighborReject[T]]
  ) =
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      connect(msg.sender) *>
        (for {
          others <- env.activeView.keys.map(_.filterNot(_ == msg.sender)).commit
          _ <- ZIO.foreachPar_(others)(
                node => send(node, Protocol.ForwardJoin(env.myself, msg.sender, TimeToLive(env.cfg.arwl))).ignore
              )
        } yield ())
    }

  private[hyparview] def handleForwardJoin[T](
    msg: Protocol.ForwardJoin[T]
  )(
    implicit
    ev1: ByteCodec[Protocol.Disconnect[T]],
    ev2: ByteCodec[Protocol.ForwardJoin[T]],
    ev3: ByteCodec[Protocol.Shuffle[T]],
    ev4: ByteCodec[Protocol.ShuffleReply[T]],
    ev5: ByteCodec[Protocol.Neighbor[T]],
    ev6: ByteCodec[Protocol.NeighborReject[T]]
  ) =
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      val process = env.activeView.keys.map(ks => (ks.size, msg.ttl.step)).flatMap {
        case (0, _) | (_, None) =>
          STM.succeed(connect(msg.originalSender))
        case (_, Some(ttl)) =>
          for {
            list <- env.activeView.keys.map(_.filterNot(_ == msg.sender))
            next <- env.selectOne(list)
            drop = if (ttl.count == env.cfg.prwl) connect(msg.sender) else ZIO.unit
          } yield drop *> ZIO.foreach(next)(send(_, msg.copy(sender = env.myself, ttl = ttl)))
      }
      process.commit.flatten
    }

  private[hyparview] def handleNeighborReject[T](
    msg: Protocol.NeighborReject[T]
  )(
    implicit
    ev1: ByteCodec[Protocol.Disconnect[T]],
    ev2: ByteCodec[Protocol.ForwardJoin[T]],
    ev3: ByteCodec[Protocol.Shuffle[T]],
    ev4: ByteCodec[Protocol.ShuffleReply[T]],
    ev5: ByteCodec[Protocol.Neighbor[T]],
    ev6: ByteCodec[Protocol.NeighborReject[T]]
  ) = disconnect(msg.sender) *> attemptPromotion

  private[hyparview] def handleShuffle[T](
    msg: Protocol.Shuffle[T]
  )(
    implicit
    ev1: ByteCodec[Protocol.Shuffle[T]],
    ev2: ByteCodec[Protocol.ShuffleReply[T]]
  ) =
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      env.activeView.keys
        .map(ks => (ks.size, msg.ttl.step))
        .flatMap {
          case (0, _) | (_, None) =>
            for {
              passive    <- env.passiveView.toList
              sentNodes  = msg.activeNodes ++ msg.passiveNodes
              replyNodes <- env.selectN(passive, env.cfg.shuffleNActive + env.cfg.shuffleNPassive)
              reply      = Protocol.ShuffleReply(replyNodes, sentNodes)
              _          <- env.addAllToPassiveView(sentNodes)
            } yield sendOneOf(msg.originalSender, reply)
          case (_, Some(ttl)) =>
            for {
              active <- env.activeView.keys
              next   <- env.selectOne(active.filterNot(_ == msg.sender))
            } yield ZIO.foreach(next)(send(_, msg.copy(sender = env.myself, ttl = ttl)))
        }
        .commit
        .flatten
    }

  private[hyparview] def handleShuffleReply[T](
    msg: Protocol.ShuffleReply[T]
  ) =
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      val sentOriginally = msg.sentOriginally.toSet
      (for {
        _         <- env.passiveView.removeIf(sentOriginally.contains)
        _         <- env.addAllToPassiveView(msg.passiveNodes)
        remaining <- env.passiveView.size.map(env.cfg.passiveViewCapacity - _)
        _         <- env.addAllToPassiveView(sentOriginally.take(remaining).toList)
      } yield ()).commit
    }

  private[hyparview] trait Env[T] {
    val env: Env.Service[T]
  }

  private[hyparview] object Env {
    def myself[T]                 = ZIO.environment[Env[T]].map(_.env.myself)
    def activeView[T]             = ZIO.environment[Env[T]].map(_.env.activeView)
    def passiveView[T]            = ZIO.environment[Env[T]].map(_.env.passiveView)
    def cfg[T]                    = ZIO.environment[Env[T]].map(_.env.cfg)
    def dropOneActiveToPassive[T] = ZIO.accessM[Env[T]](_.env.dropOneActiveToPassive.commit)

    def addNodeToActiveView[T](node: T, connection: ChunkConnection) =
      ZIO.accessM[Env[T]](_.env.addNodeToActiveView(node, connection).commit)
    def addNodeToPassiveView[T](node: T) = ZIO.accessM[Env[T]](_.env.addNodeToPassiveView(node).commit)
    def promoteRandom[T]                 = ZIO.accessM[Env[T]](_.env.promoteRandom.commit)

    trait Service[T] {
      val myself: T
      val activeView: TMap[T, ChunkConnection]
      val passiveView: TSet[T]
      val pickRandom: Int => STM[Nothing, Int]
      val cfg: Config

      val isActiveViewFull =
        activeView.keys.map(_.size >= cfg.activeViewCapactiy)

      val isPassiveViewFull =
        passiveView.size.map(_ >= cfg.passiveViewCapacity)

      val dropOneActiveToPassive =
        for {
          active  <- activeView.keys
          dropped <- selectOne(active)
          _       <- STM.foreach(dropped)(addNodeToPassiveView(_))
        } yield dropped

      def addNodeToActiveView(node: T, connection: ChunkConnection) =
        if (node == myself) STM.succeed(None)
        else {
          for {
            contained <- activeView.contains(node)
            dropped <- if (contained) STM.succeed(None)
                      else {
                        for {
                          size <- activeView.keys.map(_.size)
                          _    <- passiveView.delete(node)
                          dropped <- if (size >= cfg.activeViewCapactiy) dropOneActiveToPassive
                                    else STM.succeed(None)
                          _ <- activeView.put(node, connection)
                        } yield dropped
                      }
          } yield dropped
        }

      def addNodeToPassiveView(node: T) =
        for {
          inActive  <- activeView.contains(node)
          inPassive <- passiveView.contains(node)
        } yield {
          if (node == myself || inActive || inPassive) STM.unit
          else {
            for {
              size <- passiveView.size
              _    <- if (size >= cfg.passiveViewCapacity) dropOne(passiveView) else STM.unit
              _    <- passiveView.put(node)
            } yield ()
          }
        }

      val promoteRandom =
        for {
          activeFull <- activeView.keys.map(_.size >= cfg.activeViewCapactiy)
          promoted <- if (activeFull) STM.succeed(None)
                     else {
                       for {
                         passive  <- passiveView.toList
                         promoted <- selectOne(passive)
                       } yield promoted
                     }
        } yield promoted

      def selectOne[A](values: List[A]) =
        if (values.isEmpty) STM.succeed(None)
        else {
          for {
            index    <- pickRandom(values.size - 1)
            selected = values(index)
          } yield Some(selected)
        }

      def selectN[A](values: List[A], n: Int) =
        if (values.isEmpty) STM.succeed(Nil)
        else {
          def go(remaining: List[A], toPick: Int, acc: List[A]): STM[Nothing, List[A]] =
            (remaining, toPick) match {
              case (Nil, _) | (_, 0) => STM.succeed(acc)
              case _ =>
                pickRandom(remaining.size - 1).flatMap { index =>
                  val x  = values(index)
                  val xs = values.drop(index)
                  go(xs, toPick - 1, x :: acc)
                }
            }
          go(values, n, Nil)
        }

      def addAllToPassiveView(remaining: List[T]): STM[Nothing, Unit] = remaining match {
        case Nil     => STM.unit
        case x :: xs => addNodeToPassiveView(x) *> addAllToPassiveView(xs)
      }

      private[this] def dropOne[A](set: TSet[A]) =
        for {
          list    <- set.toList
          dropped <- selectOne(list)
          _       <- STM.foreach(dropped)(set.delete(_))
        } yield dropped

    }
  }
}
