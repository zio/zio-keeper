package zio.membership.hyparview

import zio.membership.ByteCodec
import upickle.default._
import zio.membership.ByteCodec
import upickle.default._
import zio._
import zio.stm._
import zio.membership.Error
import zio.stream.ZStream
import zio.logging.Logging

sealed private[hyparview] trait ActiveProtocol[+T]

private[hyparview] object ActiveProtocol {

  implicit def tagged[T](
    implicit
    c1: ByteCodec[Disconnect[T]],
    c2: ByteCodec[ForwardJoin[T]],
    c3: ByteCodec[Shuffle[T]],
    c4: ByteCodec[UserMessage]
  ): Tagged[ActiveProtocol[T]] =
    Tagged.instance(
      {
        case _: Disconnect[T]  => 10
        case _: ForwardJoin[T] => 11
        case _: Shuffle[T]     => 12
        case _: UserMessage    => 13
      }, {
        case 10 => c1.asInstanceOf[ByteCodec[ActiveProtocol[T]]]
        case 11 => c2.asInstanceOf[ByteCodec[ActiveProtocol[T]]]
        case 12 => c3.asInstanceOf[ByteCodec[ActiveProtocol[T]]]
        case 13 => c4.asInstanceOf[ByteCodec[ActiveProtocol[T]]]
      }
    )

  final case class Disconnect[T](
    sender: T,
    alive: Boolean
  ) extends ActiveProtocol[T]

  object Disconnect {

    implicit def codec[T: ReadWriter]: ByteCodec[Disconnect[T]] =
      ByteCodec.fromReadWriter(macroRW[Disconnect[T]])
  }

  final case class ForwardJoin[T](
    sender: T,
    originalSender: T,
    ttl: TimeToLive
  ) extends ActiveProtocol[T]

  object ForwardJoin {

    implicit def codec[T: ReadWriter]: ByteCodec[ForwardJoin[T]] =
      ByteCodec.fromReadWriter(macroRW[ForwardJoin[T]])
  }

  final case class Shuffle[T](
    sender: T,
    originalSender: T,
    activeNodes: List[T],
    passiveNodes: List[T],
    ttl: TimeToLive
  ) extends ActiveProtocol[T]

  object Shuffle {

    implicit def codec[T: ReadWriter]: ByteCodec[Shuffle[T]] =
      ByteCodec.fromReadWriter(macroRW[Shuffle[T]])
  }

  final case class UserMessage(
    msg: Chunk[Byte]
  ) extends ActiveProtocol[Nothing]

  object UserMessage {

    implicit val codec: ByteCodec[UserMessage] =
      ByteCodec.fromReadWriter(
        implicitly[ReadWriter[Array[Byte]]].bimap[UserMessage](_.msg.toArray, bA => UserMessage(Chunk.fromArray(bA)))
      )
  }

  def receiveActiveProtocol[R <: Env[T] with Logging[String], E >: Error, T](
    stream: ZStream[R, E, Chunk[Byte]],
    to: T,
    reply: Chunk[Byte] => IO[E, Unit]
  )(
    implicit
    ev1: Tagged[ActiveProtocol[T]]
  ): ZStream[R, E, Chunk[Byte]] =
    ZStream
      .managed(
        for {
          commands <- Queue.dropping[Command](256).toManaged(_.shutdown)
          offer = (c: Command) =>
            commands.offer(c).unit.catchSomeCause {
              case cause if cause.interrupted => ZIO.unit
            }
          keepInPassive <- Ref.make(false).toManaged_
          dropped <- Env
                      .using[T](_.addNodeToActiveView(to, offer).commit)
                      .toManaged { _ =>
                        keepInPassive.get.flatMap { keepInPassive =>
                          Env.using[T] { env =>
                            STM.atomically {
                              env.activeView.delete(to) *> (if (keepInPassive) env.addNodeToPassiveView(to)
                                                            else STM.unit)
                            }
                          }
                        }
                      }
          _ <- ZIO.foreach(dropped)(disconnect(_)).toManaged_
        } yield (ZStream.fromQueueWithShutdown(commands), keepInPassive)
      )
      .flatMap {
        case (commands, keepInPassive) =>
          stream.mergeEither(commands).mapM {
            case Left(raw) =>
              Tagged.read[ActiveProtocol[T]](raw).tap(c => UIO(println(c))).flatMap {
                case msg: ActiveProtocol.Disconnect[T] =>
                  keepInPassive.set(msg.alive).as((false, None))
                case msg: ActiveProtocol.ForwardJoin[T] =>
                  Env.using[T] { env =>
                    val accept =
                      sendInitial(msg.originalSender, InitialProtocol.ForwardJoinReply(env.myself))

                    val process = env.activeView.keys.map(ks => (ks.size, msg.ttl.step)).flatMap {
                      case (i, _) if i <= 1 =>
                        STM.succeed(accept)
                      case (_, None) =>
                        STM.succeed(accept)
                      case (_, Some(ttl)) =>
                        for {
                          list <- env.activeView.keys.map(_.filterNot(n => n == msg.sender || n == msg.originalSender))
                          next <- env.selectOne(list)
                          _    <- if (ttl.count == env.cfg.prwl) env.addNodeToPassiveView(msg.originalSender) else STM.unit
                        } yield ZIO.foreach(next)(send(_, msg.copy(sender = env.myself, ttl = ttl)))
                    }
                    process.commit.flatten.as((true, None))
                  }
                case msg: ActiveProtocol.Shuffle[T] =>
                  Env.using[T] { env =>
                    env.activeView.keys
                      .map(ks => (ks.size, msg.ttl.step))
                      .flatMap {
                        case (0, _) | (_, None) =>
                          for {
                            passive   <- env.passiveView.toList
                            sentNodes = msg.activeNodes ++ msg.passiveNodes
                            replyNodes <- env.selectN(
                                           passive.filterNot(_ == msg.originalSender),
                                           env.cfg.shuffleNActive + env.cfg.shuffleNPassive
                                         )
                            _ <- env.addAllToPassiveView(sentNodes)
                          } yield sendInitial(msg.originalSender, InitialProtocol.ShuffleReply(replyNodes, sentNodes))
                        case (_, Some(ttl)) =>
                          for {
                            active <- env.activeView.keys
                            next   <- env.selectOne(active.filterNot(n => n == msg.sender || n == msg.originalSender))
                          } yield ZIO.foreach(next)(send(_, msg.copy(sender = env.myself, ttl = ttl)))
                      }
                      .commit
                      .flatten
                      .as((true, None))
                  }
                case m: ActiveProtocol.UserMessage => ZIO.succeed((true, Some(m.msg)))
              }
            case Right(Command.Disconnect(shutDown)) =>
              for {
                myself <- Env.using[T](e => ZIO.succeed(e.myself))
                msg    <- Tagged.write[ActiveProtocol[T]](Disconnect(myself, shutDown))
                _      <- reply(msg)
              } yield (true, None)
            case Right(Command.Send(msg)) => reply(msg).as((true, None))
          }
      }
      .takeWhile(_._1)
      .collect {
        case (_, Some(msg)) => msg
      }
}
