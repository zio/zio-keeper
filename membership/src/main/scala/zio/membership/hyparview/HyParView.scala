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

          def handleNeighbor(msg: Protocol.Neighbor[T]) = UIO.succeed(msg).unit

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
                addNodeToActiveView(msg.sender).map((_, None))
              case (_, Some(ttl)) =>
                for {
                  dropped <- if (ttl.count == prwl) addNodeToActiveView(msg.sender) else STM.succeed(None)
                  list    <- activeView.toList.map(_.filterNot(_ == msg.sender))
                  next    <- selectOne(list)
                } yield (dropped, Some((next, msg.copy(sender = myself, ttl = ttl))))
            }
            process.commit.flatMap {
              case (dropped, forwardTo) =>
                for {
                  _ <- disconnect(dropped.toList)
                  _ <- ZIO.foreach(forwardTo) {
                        case (next, msg) => env.transport.send(next, msg.asInstanceOf[Chunk[Byte]])
                      }
                } yield ()
            }
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
