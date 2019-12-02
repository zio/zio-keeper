package zio.membership.hyparview
import zio.membership.Membership
import zio._
import zio.membership.ByteCodec
import zio.membership.transport.Transport
import zio.stm._
import zio.random.Random

import scala.util.{ Random => SRandom }

object HyParView {

  def apply[T](
    myself: T,
    randomN: TRef[Int],
    activeView: TSet[T],
    passiveView: TSet[T],
    activeViewCapactiy: Int,
    passiveViewCapacity: Int,
    arwl: Int,
    prwl: Int
  ): ZManaged[Transport[T] with Random, Nothing, Membership[T]] =
    ZManaged
      .environment[Transport[T] with Random]
      .flatMap(e => e.random.nextInt.map(seed => (new SRandom(seed), e)).toManaged_)
      .flatMap {
        case (rand, env) =>
          val pick = {
            val rStream = scala.Stream.continually((i: Int) => rand.nextInt(i))
            (max: Int) => randomN.update(_ + 1).map(rStream(_)(max))
          }

          def send(to: T, msg: Protocol[T]) = UIO.succeed((to, msg)).unit

          def selectOne[A](values: List[A]) =
            for {
              index    <- pick(values.size)
              selected = values(index)
            } yield selected

          def dropOne[A](set: TSet[A]) =
            for {
              list    <- set.toList
              dropped <- selectOne(list)
              _       <- set.delete(dropped)
            } yield dropped

          def disconnect(nodes: List[T], shutDown: Boolean = false): UIO[Unit] =
            ZIO
              .foreachPar(nodes)(
                node => send(node, Protocol.Disconnect(myself, shutDown)).ignore
              )
              .unit

          val dropOneActiveToPassive =
            for {
              dropped <- dropOne(activeView)
              _       <- passiveView.put(dropped)
            } yield dropped

          def addNodeToActiveView(node: T) =
            if (node == myself) STM.succeed(None)
            else {
              for {
                contained <- activeView.contains(node)
                dropped <- if (contained) STM.succeed(None)
                          else {
                            for {
                              size <- activeView.size
                              dropped <- if (size >= activeViewCapactiy) dropOneActiveToPassive.map(Some(_))
                                         else STM.succeed(None)
                              _ <- activeView.put(node)
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
                  _    <- if (size >= passiveViewCapacity) dropOne(passiveView) else STM.unit
                  _    <- passiveView.put(node)
                } yield ()
              }
            }

          def handleDisconnect(msg: Protocol.Disconnect[T]) =
            STM.atomically {
              for {
                inActive <- activeView.contains(msg.sender)
                _        <- if (inActive) activeView.delete(msg.sender) else STM.unit
                _        <- if (inActive && msg.alive) addNodeToPassiveView(msg.sender) else STM.unit
              } yield ()
            }

          def handleNeighbor(msg: Protocol.Neighbor[T]) =
            if (msg.isHighPriority) {
              (for {
                 dropped <- addNodeToActiveView(msg.sender)
                 _       <- passiveView.delete(msg.sender)
              } yield dropped).commit.flatMap[Any, Nothing, Unit](n => disconnect(n.toList))
            } else {
              (for {
                full <- activeView.size.map(_ == activeViewCapactiy)
                task <- if (full) {
                  for {
                    _ <- addNodeToPassiveView(msg.sender)
                  } yield send(msg.sender, Protocol.NeighborReject(myself))
                } else {
                  for {
                    dropped <- addNodeToActiveView(msg.sender)
                    _       <- passiveView.delete(msg.sender)
                  } yield disconnect(dropped.toList)
                }
              } yield task).commit.flatten
            }

          def handleJoin(msg: Protocol.Join[T]) =
            (for {
              _      <- addNodeToActiveView(msg.sender)
              others <- activeView.toList.map(_.filterNot(_ == msg.sender))
            } yield others).commit.flatMap(
              nodes =>
                ZIO.foreachPar(nodes) { node =>
                  send(node, Protocol.ForwardJoin(myself, msg.sender, TimeToLive(arwl))).ignore
                }
            )

          def handleForwardJoin(msg: Protocol.ForwardJoin[T]) = {
            val process = activeView.size.map((_, msg.ttl.step)).flatMap {
              case (0, _) | (_, None) =>
                addNodeToActiveView(msg.sender).map(n => disconnect(n.toList))
              case (_, Some(ttl)) =>
                for {
                  drop <- if (ttl.count == prwl) {
                    addNodeToActiveView(msg.sender).map(n => disconnect(n.toList))
                  } else STM.succeed(ZIO.unit)
                  list    <- activeView.toList.map(_.filterNot(_ == msg.sender))
                  next    <- selectOne(list)
                } yield drop *> send(next, msg.copy(sender = myself, ttl = ttl))
            }
            process.commit.flatten
          }

          //  def handleShuffle(shuffle: Protocol.Shuffle[T]) = UIO.unit
          //  def handleShuffleReply(shuffleReply: Protocol.ShuffleReply[T]) = UIO.unit

          ZManaged.make {
            for {
              _ <- env.transport
                    .bind(myself)
                    .foreach { msg =>
                      Protocol.decodeChunk[T](msg).flatMap {
                        case m: Protocol.Neighbor[T]     => handleNeighbor(m).fork
                        case m: Protocol.Disconnect[T]   => handleDisconnect(m)
                        case m: Protocol.Join[T]         => handleJoin(m).fork
                        case m: Protocol.ForwardJoin[T]  => handleForwardJoin(m).fork
                        case _: Protocol.Shuffle[T]      => ???
                        case _: Protocol.ShuffleReply[T] => ???
                        case _: Protocol.RefuseNeighbor[T] => ???
                      }
                    }
                    .fork
            } yield {

              new Membership[T] {

                override val membership = new Membership.Service[Any, T] {

                  override val identity = ZIO.succeed(myself)

                  override val nodes = activeView.toList.commit

                  override def send[R1, A](to: T, payload: A)(implicit ev: ByteCodec[R1, A]) = ???

                  override def broadcast[R1, A](payload: A)(implicit ev: ByteCodec[R1, A]) = ???

                  override def receive[R1, A](implicit ev: ByteCodec[R1, A]) = ???
                }
              }
            }
          } { _ =>
            activeView.toList.commit.flatMap(disconnect(_, true))
          }
      }

  final case class Config(
    activeViewCapactiy: Int,
    passiveViewCapacity: Int,
    arwl: Int,
    prwl: Int
  )

  //=========
  // internal
  //=========

  private[hyparview] type Handler[-R[_], M[_], T] = M[T] => ZIO[R[T], Nothing, Unit]

  private[hyparview] def send[T](to: T, msg: Protocol[T]): ZIO[Transport[T], Nothing, Unit] = UIO.succeed((to, msg)).unit

  private[hyparview] def disconnect[T](node: T, shutDown: Boolean = false): ZIO[Transport[T] with Env[T], Nothing, Unit] =
    ZIO.environment[Env[T]].map(_.env.myself).flatMap { myself =>
      send(node, Protocol.Disconnect(myself, shutDown)).ignore.unit
    }

  private[hyparview] def handleDisconnect[T]: Handler[({ type R[X] = Env[X] with Transport[X]})#R, Protocol.Disconnect, T] = { msg =>
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      STM.atomically {
        for {
          inActive <- env.activeView.contains(msg.sender)
          _        <- if (inActive) env.activeView.delete(msg.sender) else STM.unit
          _        <- if (inActive && msg.alive) env.addNodeToPassiveView(msg.sender) else STM.unit
        } yield msg.sender
      }.flatMap(disconnect(_))
    }
  }

  private[hyparview] def handleNeighbor[T]: Handler[({ type R[X] = Env[X] with Transport[X]})#R, Protocol.Neighbor, T] = { msg =>
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      if (msg.isHighPriority) {
        (for {
          dropped <- env.addNodeToActiveView(msg.sender)
          _       <- env.passiveView.delete(msg.sender)
        } yield dropped).commit.flatMap(n => ZIO.foreach(n)(disconnect(_))).unit
      } else {
        (for {
          full <- env.activeView.size.map(_ == env.cfg.activeViewCapactiy)
          task <- if (full) {
            for {
              _ <- env.addNodeToPassiveView(msg.sender)
            } yield send(msg.sender, Protocol.NeighborReject(env.myself))
          } else {
            for {
              dropped <- env.addNodeToActiveView(msg.sender)
              _       <- env.passiveView.delete(msg.sender)
            } yield ZIO.foreach(dropped)(disconnect(_)).unit
          }
        } yield task).commit.flatten
      }
    }
  }

  private[hyparview] def handleJoin[T]: Handler[({ type R[X] = Env[X] with Transport[X]})#R, Protocol.Join, T] = { msg =>
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      (for {
        _      <- env.addNodeToActiveView(msg.sender)
        others <- env.activeView.toList.map(_.filterNot(_ == msg.sender))
      } yield others).commit.flatMap(
        nodes =>
          ZIO.foreachPar(nodes) { node =>
            send(node, Protocol.ForwardJoin(env.myself, msg.sender, TimeToLive(env.cfg.arwl))).ignore
          }.unit
      )
    }
  }

  private[hyparview] def handleForwardJoin[T]: Handler[({ type R[X] = Env[X] with Transport[X]})#R, Protocol.ForwardJoin, T] = { msg =>
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      val process = env.activeView.size.map((_, msg.ttl.step)).flatMap {
        case (0, _) | (_, None) =>
          env.addNodeToActiveView(msg.sender).map(n => disconnect(n.toList))
        case (_, Some(ttl)) =>
          for {
            drop <- if (ttl.count == env.cfg.prwl) {
              env.addNodeToActiveView(msg.sender).map(n => disconnect(n.toList))
            } else STM.succeed(ZIO.unit)
            list    <- env.activeView.toList.map(_.filterNot(_ == msg.sender))
            next    <- env.selectOne(list)
          } yield drop *> send(next, msg.copy(sender = env.myself, ttl = ttl))
      }
      process.commit.flatten
    }
  }

  private[hyparview] def handleNeighborReject[T]: Handler[({ type R[X] = Env[X] with Transport[X]})#R, Protocol.NeighborReject, T] = { msg =>
    ZIO.environment[Env[T]].map(_.env).flatMap { env =>
      for {
        _ <- env.activeView.delete(msg.sender)
        _ <- env.promoteRandom
        nActive <- env.activeView.size
      } yield ???
    }
  }

  private[hyparview] trait Env[T] {
    def env: Env.Service[T]
  }

  object Env {
    trait Service[T] {
      val myself: T
      val activeView: TSet[T]
      val passiveView: TSet[T]
      val pickRandom: Int => STM[Nothing, Int]
      val cfg: Config

      val dropOneActiveToPassive =
        for {
          dropped <- dropOne(activeView)
          _       <- passiveView.put(dropped)
        } yield dropped

      def addNodeToActiveView(node: T) =
        if (node == myself) STM.succeed(None)
        else {
          for {
            contained <- activeView.contains(node)
            dropped <- if (contained) STM.succeed(None)
                      else {
                        for {
                          size <- activeView.size
                          dropped <- if (size >= cfg.activeViewCapactiy) dropOneActiveToPassive.map(Some(_))
                                     else STM.succeed(None)
                          _ <- activeView.put(node)
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
          passive <- passiveView.toList
          promoted <- selectOne(passive)
          _         <- passiveView.delete(promoted)
          dropped   <- addNodeToActiveView(promoted)
          _         <- STM.foreach(dropped)(addNodeToPassiveView)
        } yield promoted

      def selectOne[A](values: List[A]) =
        for {
          index    <- pickRandom(values.size)
          selected = values(index)
        } yield selected

      def dropOne[A](set: TSet[A]) =
        for {
          list    <- set.toList
          dropped <- selectOne(list)
          _       <- set.delete(dropped)
        } yield dropped

    }
  }
}
