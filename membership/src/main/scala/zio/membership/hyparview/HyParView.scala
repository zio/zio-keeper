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
                node => env.transport.send(node, Protocol.Disconnect(myself, shutDown).asInstanceOf[Chunk[Byte]]).ignore
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
                  } yield env.transport.send(msg.sender, Protocol.RefuseNeighbor(myself).asInstanceOf[Chunk[Byte]])
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
                  env.transport
                    .send(node, Protocol.ForwardJoin(myself, msg.sender, TimeToLive(arwl)).asInstanceOf[Chunk[Byte]])
                    .ignore
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
                } yield drop *> env.transport.send(next, msg.copy(sender = myself, ttl = ttl).asInstanceOf[Chunk[Byte]])
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
                        case m: Protocol.Neighbor[T]     => handleNeighbor(m)
                        case m: Protocol.Disconnect[T]   => handleDisconnect(m)
                        case m: Protocol.Join[T]         => handleJoin(m)
                        case m: Protocol.ForwardJoin[T]  => handleForwardJoin(m)
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
}
