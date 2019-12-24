package zio.membership.hyparview

import zio.membership.ByteCodec
import upickle.default._
import zio._
import zio.stm._
import zio.membership.transport._
import zio.membership.Error
import zio.stream.ZStream
import zio.logging.Logging
import zio.membership.log

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

  def send[R <: Transport[T] with Logging[String] with Views[T], R1 <: R, E >: Error, E1 >: E, T, A](
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
      .tap { case (to, msg) => log.debug(s"sendInitialProtocol: $to -> $msg") }
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
                NeighborProtocol.receive[R, R1, E, E1, A](con.receive) { cont =>
                  contStream(to, con.send, cont)
                }
              }
            }
            .orElse(ZStream.fromEffect(Views.using[T](_.removeFromPassiveView(to).commit)) *> ZStream.empty)
        case (sender, m: ShuffleReply[T]) =>
          ZStream
            .unwrapManaged {
              openConnection(sender, m).as(ZStream.empty)
            }
            .orElse(ZStream.empty)
      }
  }

  def receive[R <: Views[T] with Transport[T] with Logging[String] with Cfg, R1 <: R, E >: Error, E1 >: E, T, A](
    stream: ZStream[R, E, Chunk[Byte]],
    sendReply: Chunk[Byte] => IO[E, Unit]
  )(
    contStream: (T, ZStream[R, E, Chunk[Byte]]) => ZStream[R1, E1, A]
  )(
    implicit
    ev1: Tagged[InitialProtocol[T]],
    ev2: ByteCodec[JoinReply[T]]
  ): ZStream[R1, E1, A] =
    ZStream.unwrapManaged {
      stream.process.mapM { pull =>
        val continue =
          pull.foldM[R, E, Option[T]](
            _.fold[ZIO[R, E, Option[T]]](ZIO.succeed(None))(ZIO.fail(_)), { raw =>
              Tagged
                .read[InitialProtocol[T]](raw)
                .tap(msg => log.debug(s"receiveInitialProtocol: $msg"))
                .flatMap {
                  case msg: InitialProtocol.Neighbor[T] =>
                    Views.using[T] {
                      views =>
                        val accept = for {
                          reply <- Tagged.write[NeighborProtocol](NeighborProtocol.Accept)
                          _     <- log.debug(s"Accepting neighborhood request from ${msg.sender}")
                          _     <- sendReply(reply)
                        } yield Some(msg.sender)

                        val reject = for {
                          reply <- Tagged.write[NeighborProtocol](NeighborProtocol.Reject)
                          _     <- log.debug(s"Rejecting neighborhood request from ${msg.sender}")
                          _     <- sendReply(reply)
                        } yield None

                        if (msg.isHighPriority) {
                          accept
                        } else {
                          STM.atomically {
                            for {
                              full <- views.isActiveViewFull
                              task <- if (full) {
                                       views
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
                    Views.using[T] { views =>
                      for {
                        others <- views.activeView.map(_.filterNot(_ == msg.sender)).commit
                        config <- getConfig
                        _ <- ZIO
                              .foreachPar_(others)(
                                node =>
                                  views
                                    .send(
                                      node,
                                      ActiveProtocol.ForwardJoin(views.myself, msg.sender, TimeToLive(config.arwl))
                                    )
                                    .ignore
                              )
                        reply <- ByteCodec[JoinReply[T]].toChunk(JoinReply(views.myself))
                        _     <- sendReply(reply)
                      } yield Some(msg.sender)
                    }
                  case msg: InitialProtocol.ForwardJoinReply[T] => ZIO.succeed(Some(msg.sender)) // nothing to do here
                  case msg: InitialProtocol.ShuffleReply[T] =>
                    Views.using[T] { views =>
                      views
                        .addShuffledNodes(msg.sentOriginally.toSet, msg.passiveNodes.toSet)
                        .commit
                        .as(None)
                    }
                }
            }
          )
        continue.map(_.fold[ZStream[R1, E1, A]](ZStream.empty)(contStream(_, ZStream.fromPull(pull))))
      }
    }
}
