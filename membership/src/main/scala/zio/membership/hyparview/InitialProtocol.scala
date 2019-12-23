package zio.membership.hyparview

import zio.membership.ByteCodec
import upickle.default._
import zio._
import zio.stm._
import zio.membership.transport._
import zio.membership.Error
import zio.stream.ZStream
import zio.logging.Logging

sealed private[hyparview] trait InitialProtocol[T]

private[hyparview] object InitialProtocol {

  implicit def tagged[T](
    implicit
    c1: ByteCodec[Neighbor[T]],
    c2: ByteCodec[Join[T]],
    c3: ByteCodec[ForwardJoinReply[T]],
    c4: ByteCodec[ShuffleReply[T]]
  ): Tagged[InitialProtocol[T]] =
    Tagged.instance(
      {
        case _: Neighbor[T]         => 0
        case _: Join[T]             => 1
        case _: ForwardJoinReply[T] => 2
        case _: ShuffleReply[T]     => 3
      }, {
        case 0 => c1.asInstanceOf[ByteCodec[InitialProtocol[T]]]
        case 1 => c2.asInstanceOf[ByteCodec[InitialProtocol[T]]]
        case 2 => c3.asInstanceOf[ByteCodec[InitialProtocol[T]]]
        case 3 => c4.asInstanceOf[ByteCodec[InitialProtocol[T]]]
      }
    )

  final case class Neighbor[T](
    sender: T,
    isHighPriority: Boolean
  ) extends InitialProtocol[T]

  object Neighbor {

    implicit def codec[T: ReadWriter]: ByteCodec[Neighbor[T]] =
      ByteCodec.fromReadWriter(macroRW[Neighbor[T]])
  }

  final case class Join[T](
    sender: T
  ) extends InitialProtocol[T]

  object Join {

    implicit def codec[T: ReadWriter]: ByteCodec[Join[T]] =
      ByteCodec.fromReadWriter(macroRW[Join[T]])
  }

  final case class ForwardJoinReply[T](
    sender: T
  ) extends InitialProtocol[T]

  object ForwardJoinReply {

    implicit def codec[T: ReadWriter]: ByteCodec[ForwardJoinReply[T]] =
      ByteCodec.fromReadWriter(macroRW[ForwardJoinReply[T]])
  }

  final case class ShuffleReply[T](
    passiveNodes: List[T],
    sentOriginally: List[T]
  ) extends InitialProtocol[T]

  object ShuffleReply {

    implicit def codec[T: ReadWriter]: ByteCodec[ShuffleReply[T]] =
      ByteCodec.fromReadWriter(macroRW[ShuffleReply[T]])
  }

  def sendInitialProtocol[R <: Env[T] with Transport[T], R1 <: R, E >: Error, E1 >: E, T, A](
    stream: ZStream[R, E, (T, InitialProtocol[T])]
  )(
    contStream: (T, Chunk[Byte] => IO[E, Unit], ZStream[R, E, Chunk[Byte]]) => ZStream[R1, E1, A]
  )(
    implicit
    ev1: Tagged[InitialProtocol[T]],
    ev2: ByteCodec[JoinReply[T]]
  ): ZStream[R1, E, ZStream[R1, E1, A]] = {
    def openConnection(to: T, msg: InitialProtocol[T]) =
      for {
        con <- Transport.connect(to)
        msg <- Tagged.write[InitialProtocol[T]](msg).toManaged_
        _   <- con.send(msg).toManaged_
      } yield con

    stream
      .map {
        case (to, m: ForwardJoinReply[T]) =>
          ZStream
            .unwrapManaged {
              openConnection(to, m).map { con =>
                contStream(to, con.send, con.receive)
              }
            }
            .orElse(ZStream.empty)
        case (to, m: Join[T]) =>
          ZStream
            .unwrapManaged {
              openConnection(to, m).map { con =>
                JoinReply.receive[R, R1, E, E1, T, A](con.receive) {
                  case (sender, receive) =>
                    contStream(sender, con.send, receive)
                }
              }
            }
            .orElse(ZStream.empty)
        case (to, m: Neighbor[T]) =>
          ZStream
            .unwrapManaged {
              openConnection(to, m).map { con =>
                NeighborProtocol.receiveNeighborProtocol[R, R1, E, E1, A](con.receive) { cont =>
                  contStream(to, con.send, cont)
                }
              }
            }
            .orElse(ZStream.fromEffect(Env.using[T](_.passiveView.delete(to).commit)) *> ZStream.empty)
        case (sender, m: ShuffleReply[T]) =>
          ZStream
            .unwrapManaged {
              openConnection(sender, m).as(ZStream.empty)
            }
            .orElse(ZStream.empty)
      }
  }

  def receiveInitialProtocol[R <: Env[T] with Transport[T] with Logging[String], R1 <: R, E >: Error, E1 >: E, T, A](
    stream: ZStream[R, E, Chunk[Byte]],
    sendReply: Chunk[Byte] => IO[E, Unit]
  )(
    contStream: (T, ZStream[R, E, Chunk[Byte]]) => ZStream[R1, E1, A]
  )(
    implicit
    ev1: Tagged[InitialProtocol[T]],
    ev2: Tagged[ActiveProtocol[T]],
    ev3: ByteCodec[JoinReply[T]]
  ): ZStream[R1, E1, A] =
    ZStream.unwrapManaged {
      stream.process.mapM { pull =>
        val continue =
          pull.foldM[R, E, Option[T]](
            _.fold[ZIO[R, E, Option[T]]](ZIO.succeed(None))(ZIO.fail(_)), { raw =>
              Tagged.read[InitialProtocol[T]](raw).flatMap {
                case msg: InitialProtocol.Neighbor[T] =>
                  Env.using[T] {
                    env =>
                      val accept = for {
                        reply <- Tagged.write[NeighborProtocol](NeighborProtocol.Accept)
                        _     <- sendReply(reply)
                      } yield Some(msg.sender)

                      val reject = for {
                        reply <- Tagged.write[NeighborProtocol](NeighborProtocol.Reject)
                        _     <- sendReply(reply)
                      } yield None

                      if (msg.isHighPriority) {
                        accept
                      } else {
                        STM.atomically {
                          for {
                            full <- env.isActiveViewFull
                            task <- if (full) {
                                     env
                                       .addNodeToPassiveView(msg.sender)
                                       .as(
                                         reject
                                       )
                                   } else {
                                     STM.succeed(accept)
                                   }
                          } yield task
                        }.flatten
                      }
                  }
                case msg: InitialProtocol.Join[T] =>
                  Env.using[T] { env =>
                    for {
                      others <- env.activeView.keys.map(_.filterNot(_ == msg.sender)).commit
                      _ <- ZIO
                            .foreachPar_(others)(
                              node =>
                                send(
                                  node,
                                  ActiveProtocol.ForwardJoin(env.myself, msg.sender, TimeToLive(env.cfg.arwl))
                                ).ignore
                            )
                      reply <- ByteCodec[JoinReply[T]].toChunk(JoinReply(env.myself))
                      _     <- sendReply(reply)
                    } yield Some(msg.sender)
                  }
                case msg: InitialProtocol.ForwardJoinReply[T] => ZIO.succeed(Some(msg.sender)) // nothing to do here
                case msg: InitialProtocol.ShuffleReply[T] =>
                  Env.using[T] { env =>
                    val sentOriginally = msg.sentOriginally.toSet
                    STM.atomically {
                      for {
                        _         <- env.passiveView.removeIf(sentOriginally.contains)
                        _         <- env.addAllToPassiveView(msg.passiveNodes)
                        remaining <- env.passiveView.size.map(env.cfg.passiveViewCapacity - _)
                        _         <- env.addAllToPassiveView(sentOriginally.take(remaining).toList)
                      } yield None
                    }
                  }
              }
            }
          )
        continue.map(_.fold[ZStream[R1, E1, A]](ZStream.empty)(contStream(_, ZStream.fromPull(pull))))
      }
    }
}
