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
import zio.membership.log

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

  def receiveActiveProtocol[R <: Views[T] with Cfg with Logging[String], E >: Error, T](
    stream: ZStream[R, E, Chunk[Byte]],
    to: T,
    reply: Chunk[Byte] => IO[E, Unit],
    sendInitial: (T, InitialProtocol[T]) => IO[E, Unit]
  )(
    implicit
    ev1: Tagged[ActiveProtocol[T]]
  ): ZStream[R, E, Chunk[Byte]] =
    ZStream
      .managed(
        for {
          env           <- ZManaged.environment[R]
          keepInPassive <- Ref.make(false).toManaged_
          finalizerRef  <- ZManaged.finalizerRef(_ => UIO.unit)
          end           <- Promise.make[Nothing, Nothing].toManaged_
          config        <- getConfig.toManaged_
          _ <- Views
                .using[T] { views =>
                  views.addNodeToActiveView(
                    to,
                    msg =>
                      (for {
                        chunk <- Tagged.write[ActiveProtocol[T]](msg)
                        _     <- reply(chunk)
                        _     <- log.debug(s"sendActiveProtocol: $to -> $msg")
                      } yield ())
                        .tapError(e => log.error("Failed sending activeProtocol message", Cause.fail(e)))
                        .mapError(_ => ())
                        .provide(env),
                    end.interrupt.unit,
                    f => finalizerRef.set(_ => keepInPassive.get.flatMap(f))
                  )
                }
                .toManaged_
        } yield (keepInPassive, end, config)
      )
      .flatMap {
        case (keepInPassive, end, config) =>
          stream
            .merge(ZStream.fromEffect(end.await))
            .mapM { raw =>
              Tagged
                .read[ActiveProtocol[T]](raw)
                .tap(msg => log.debug(s"receiveActiveProtocol: $to -> $msg"))
                .flatMap {
                  case msg: ActiveProtocol.Disconnect[T] =>
                    keepInPassive.set(msg.alive).as((false, None))
                  case msg: ActiveProtocol.ForwardJoin[T] =>
                    Views.using[T] { views =>
                      val accept =
                        sendInitial(msg.originalSender, InitialProtocol.ForwardJoinReply(views.myself))

                      val process = views.activeViewSize.map[(Int, Option[TimeToLive])]((_, msg.ttl.step)).flatMap {
                        case (i, _) if i <= 1 =>
                          STM.succeed(accept)
                        case (_, None) =>
                          STM.succeed(accept)
                        case (_, Some(ttl)) =>
                          for {
                            list <- views.activeView
                                     .map(_.filterNot(n => n == msg.sender || n == msg.originalSender).toList)
                            _ <- if (ttl.count == config.prwl) views.addNodeToPassiveView(msg.originalSender)
                                else STM.unit
                            forward = msg.copy(sender = views.myself, ttl = ttl)
                          } yield {
                            def go(candidates: List[T]): UIO[Unit] =
                              views
                                .selectOne(candidates)
                                .commit
                                .flatMap(
                                  _.fold(ZIO.unit)(c => views.send(c, forward).orElse(go(candidates.filterNot(_ == c))))
                                )
                            go(list)
                          }
                      }
                      process.commit.flatten.as((true, None))
                    }
                  case msg: ActiveProtocol.Shuffle[T] =>
                    Views.using[T] { views =>
                      views.activeViewSize
                        .map[(Int, Option[TimeToLive])]((_, msg.ttl.step))
                        .flatMap {
                          case (0, _) | (_, None) =>
                            for {
                              passive   <- views.passiveView
                              sentNodes = msg.activeNodes ++ msg.passiveNodes
                              replyNodes <- views.selectN(
                                             passive.filterNot(_ == msg.originalSender).toList,
                                             config.shuffleNActive + config.shuffleNPassive
                                           )
                              _ <- views.addAllToPassiveView(sentNodes)
                            } yield sendInitial(
                              msg.originalSender,
                              InitialProtocol.ShuffleReply(replyNodes, sentNodes)
                            )
                          case (_, Some(ttl)) =>
                            for {
                              active <- views.activeView.map(
                                         _.filterNot(n => n == msg.sender || n == msg.originalSender).toList
                                       )
                              forward = msg.copy(sender = views.myself, ttl = ttl)
                            } yield {
                              def go(candidates: List[T]): UIO[Unit] =
                                views
                                  .selectOne(candidates)
                                  .commit
                                  .flatMap(
                                    _.fold(ZIO.unit)(
                                      c => views.send(c, forward).orElse(go(candidates.filterNot(_ == c)))
                                    )
                                  )
                              go(active)
                            }
                        }
                        .commit
                        .flatten
                        .as((true, None))
                    }
                  case m: ActiveProtocol.UserMessage => ZIO.succeed((true, Some(m.msg)))
                }
            }
      }
      .takeWhile(_._1)
      .collect {
        case (_, Some(msg)) => msg
      }
}
