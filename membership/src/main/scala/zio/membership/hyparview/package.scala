package zio.membership

import zio._
import zio.logging._
import zio.keeper.membership.{ ByteCodec, TaggedCodec }
import zio.logging.Logging.Logging
import zio.membership.hyparview.NeighborReply.{ Accept, Reject }
import zio.membership.transport.{ ChunkConnection, Transport }
import zio.stm.STM
import zio.stream._

package object hyparview {

  type TRandom = Has[TRandom.Service]

  type HyParViewConfig = Has[HyParViewConfig.Service]

  private[hyparview] type Views[T] = Has[Views.Service[T]]

  type Enqueue[-A] = ZQueue[Any, Nothing, Any, Nothing, A, Any]

  val getConfig: URIO[HyParViewConfig, HyParViewConfig.Config] =
    URIO.accessM[HyParViewConfig](_.get.getConfig)

  private[hyparview] def readJoinReply[R, E >: DeserializationError, T](
    stream: ZStream[R, E, Chunk[Byte]]
  )(
    implicit ev: ByteCodec[JoinReply[T]]
  ): ZManaged[R, E, Option[(T, ZStream[R, E, Chunk[Byte]])]] =
    stream.process.mapM { pull =>
      val continue =
        pull.foldM[R, E, Option[T]](
          _.fold[ZIO[R, E, Option[T]]](ZIO.succeed(None))(ZIO.fail(_)), { msg =>
            ByteCodec[JoinReply[T]]
              .fromChunk(msg)
              .mapError(e => zio.membership.DeserializationError(e.msg))
              .map {
                case JoinReply(addr) => Some(addr)
              }
          }
        )
      continue.map(_.map((_, ZStream.repeatEffectOption(pull))))
    }

  private[hyparview] def readNeighborReply[R, E >: DeserializationError, A](
    stream: ZStream[R, E, Chunk[Byte]]
  ): ZManaged[R, E, Option[Stream[E, Chunk[Byte]]]] =
    stream.process.mapM { pull =>
      val continue =
        pull.foldM[R, E, Boolean](
          _.fold[ZIO[R, E, Boolean]](ZIO.succeed(false))(ZIO.fail(_)), { msg =>
            TaggedCodec
              .read[NeighborReply](msg)
              .mapError(e => zio.membership.DeserializationError(e.msg))
              .map {
                case Accept => true
                case Reject => false
              }
          }
        )
      ZIO.environment[R].flatMap { env =>
        continue.map(if (_) Some(ZStream.repeatEffectOption(pull).provide(env)) else None)
      }
    }

  private[hyparview] def sendInitial[T: Tagged](
    to: T,
    msg: InitialMessage[T],
    allocate: ScopeIO,
    connections: Enqueue[(T, Chunk[Byte] => IO[TransportError, Unit], Stream[Error, Chunk[Byte]], UIO[_])]
  )(
    implicit
    ev1: TaggedCodec[InitialProtocol[T]],
    ev2: ByteCodec[JoinReply[T]]
  ): ZIO[Logging with Transport[T], Error, Unit] =
    log.debug(s"sendInitial $to -> $msg") *> (allocate {
      import InitialMessage._
      def openConnection(to: T, msg: InitialProtocol[T]) =
        for {
          con <- transport.connect(to)
          msg <- TaggedCodec.write[InitialProtocol[T]](msg).toManaged_
          _   <- con.send(msg).toManaged_
        } yield con

      msg match {
        case m: ForwardJoinReply[T] =>
          openConnection(to, m).map { con =>
            Some((to, con.send(_), con.receive))
          }
        case m: Join[T] =>
          openConnection(to, m).flatMap { con =>
            readJoinReply(con.receive).map(_.map { case (addr, receive) => (addr, con.send(_), receive) })
          }
        case m: ShuffleReply[T] =>
          openConnection(to, m).as(None)
      }
    }).foldM(
      e =>
        log.error(
          "Error in Initial",
          Cause.Both(Cause.fail(e), Cause.fail(s"Failed sending initialMessage $msg to $to"))
        ), {
        case (None, release) =>
          release.unit
        case (Some((addr, send, receive)), release) =>
          connections.offer((addr, send, receive, release)).unit
      }
    )

  private[hyparview] def receiveInitialProtocol[R <: Views[T] with Transport[T] with Logging with HyParViewConfig, E >: Error, T: Tagged](
    stream: ZStream[R, E, Managed[Nothing, ChunkConnection]],
    concurrentConnections: Int = 16
  )(
    implicit
    ev1: TaggedCodec[InitialProtocol[T]],
    ev2: ByteCodec[JoinReply[T]]
  ): ZStream[R, E, (T, Chunk[Byte] => IO[TransportError, Unit], Stream[Error, Chunk[Byte]], UIO[_])] =
    ZStream.managed(ScopeIO.make).flatMap { allocate =>
      stream
        .mapMPar(concurrentConnections) { conM =>
          allocate {
            conM
              .flatMap { con =>
                con.receive.process.mapM[R, E, Option[
                  (T, Chunk[Byte] => IO[TransportError, Unit], Stream[Error, Chunk[Byte]])
                ]] { pull =>
                  pull
                    .foldM(
                      _.fold[ZIO[R, E, Option[T]]](ZIO.succeed(None))(ZIO.fail(_)), { raw =>
                        TaggedCodec
                          .read[InitialProtocol[T]](raw)
                          .mapError(e => zio.membership.DeserializationError(e.msg))
                          .tap(msg => log.debug(s"receiveInitialProtocol: $msg"))
                          .flatMap {
                            case msg: Neighbor[T] =>
                              Views.using[T].apply {
                                views =>
                                  val accept = for {
                                    reply <- TaggedCodec
                                              .write[NeighborReply](NeighborReply.Accept)
                                              .mapError(e => zio.membership.SerializationError(e.msg))
                                    _ <- log.debug(s"Accepting neighborhood request from ${msg.sender}")
                                    _ <- con.send(reply)
                                  } yield Some(msg.sender)

                                  val reject = for {
                                    reply <- TaggedCodec
                                              .write[NeighborReply](NeighborReply.Reject)
                                              .mapError(e => zio.membership.SerializationError(e.msg))
                                    _ <- log.debug(s"Rejecting neighborhood request from ${msg.sender}")
                                    _ <- con.send(reply)
                                  } yield None

                                  if (msg.isHighPriority) {
                                    accept
                                  } else {
                                    STM.atomically {
                                      for {
                                        full <- views.isActiveViewFull
                                        task <- if (full) {
                                                 views
                                                   .addToPassiveView(msg.sender)
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
                            case msg: InitialMessage.Join[T] =>
                              Views.using[T].apply {
                                views =>
                                  for {
                                    others <- views.activeView.map(_.filterNot(_ == msg.sender)).commit
                                    config <- getConfig
                                    _ <- ZIO
                                          .foreachPar_(others)(
                                            node =>
                                              views
                                                .send(
                                                  node,
                                                  ActiveProtocol
                                                    .ForwardJoin(views.myself, msg.sender, TimeToLive(config.arwl))
                                                )
                                          )
                                    reply <- ByteCodec[JoinReply[T]]
                                              .toChunk(JoinReply(views.myself))
                                              .mapError(e => zio.membership.SerializationError(e.msg))
                                    _ <- con.send(reply)
                                  } yield Some(msg.sender)
                              }
                            case msg: InitialMessage.ForwardJoinReply[T] =>
                              // nothing to do here, we just continue to the next protocol
                              ZIO.succeed(Some(msg.sender))
                            case msg: InitialMessage.ShuffleReply[T] =>
                              Views.using[T].apply { views =>
                                views
                                  .addShuffledNodes(msg.sentOriginally.toSet, msg.passiveNodes.toSet)
                                  .commit
                                  .as(None)
                              }
                          }
                      }
                    )
                    .map(_.map((_, con.send(_), ZStream.repeatEffectOption(pull))))
                }
              }

          }.foldM(
            e =>
              log
                .error(
                  "Error in Receive",
                  Cause.Both(Cause.fail(e), Cause.fail("Failure while running initial protocol"))
                )
                .as(None), {
              case (None, release)                        => release.as(None)
              case (Some((addr, send, receive)), release) => ZIO.succeed(Some((addr, send, receive, release)))
            }
          )
        }
        .collect {
          case Some(x) => x
        }
    }

  private[hyparview] def neighborProtocol[T: Tagged](
    implicit ev: TaggedCodec[InitialProtocol[T]]
  ): ZStream[
    Views[T] with Transport[T] with Logging with TRandom,
    Nothing,
    (T, Chunk[Byte] => IO[TransportError, Unit], Stream[Error, Chunk[Byte]], UIO[_])
  ] =
    ZStream
      .managed(
        ScopeIO.make
      )
      .flatMap { preallocate =>
        ZStream
          .repeatEffect[Views[T] with TRandom, Nothing, (T, Neighbor[T])] {
            Views.using[T].apply { views =>
              TRandom.using { tRandom =>
                STM.atomically {
                  for {
                    activeViewSize <- views.activeViewSize
                    _              <- STM.check(activeViewSize < views.activeViewCapacity)
                    nodeOpt        <- views.passiveView.flatMap(xs => tRandom.selectOne(xs.toList))
                    node           <- nodeOpt.fold[STM[Nothing, T]](STM.retry)(STM.succeed(_))
                  } yield (node, Neighbor(views.myself, activeViewSize <= 0))
                }
              }
            }
          }
          .mapM {
            case (node, msg) =>
              preallocate {
                for {
                  _    <- log.debug(s"Running neighbor protocol with remote $node").toManaged_
                  con  <- transport.connect(node)
                  msg  <- TaggedCodec.write[InitialProtocol[T]](msg).toManaged_
                  _    <- con.send(msg).toManaged_
                  cont <- readNeighborReply(con.receive)
                } yield cont.map((node, con.send(_), _))
              }.foldM(
                e =>
                  for {
                    _ <- log.error(s"Failed neighbor protocol with remote $node", Cause.fail(e))
                    _ <- Views.using[T].apply(_.removeFromPassiveView(node).commit)
                  } yield None, {
                  case (None, release)                        => release.as(None)
                  case (Some((addr, send, receive)), release) => ZIO.succeed(Some((addr, send, receive, release)))
                }
              )
          }
          .collect {
            case Some(x) => x
          }
      }

  private[hyparview] def runActiveProtocol[R <: Views[T] with HyParViewConfig with Logging with TRandom, E >: Error, T: Tagged](
    stream: ZStream[R, E, Chunk[Byte]],
    to: T,
    reply: Chunk[Byte] => IO[TransportError, Unit],
    sendInitial: (T, InitialMessage[T]) => IO[E, Unit]
  )(
    implicit
    ev: TaggedCodec[ActiveProtocol[T]]
  ): ZStream[R, E, Chunk[Byte]] = {
    import ActiveProtocol._
    ZStream
      .managed(
        for {
          env <- ZManaged.environment[R]
          end <- Promise.make[Unit, Nothing].toManaged_
          keepInPassive <- {
            for {
              _ <- Views.using[T].apply { views =>
                    views
                      .addToActiveView(
                        to,
                        msg =>
                          (for {
                            chunk <- TaggedCodec.write[ActiveProtocol[T]](msg).mapError(SendError.SerializationFailed)
                            _     <- reply(chunk).mapError(SendError.TransportFailed)
                            _     <- log.debug(s"sendActiveProtocol: $to -> $msg")
                          } yield ())
                            .tapError(
                              e => log.error(s"Failed sending message $msg to $to", Cause.fail(e))
                            )
                            .provide(env),
                        end.fail(()).unit
                      )
                      .commit
                  }
              ref <- Ref.make(false)
            } yield ref
          }.toManaged(_.get.flatMap {
            case true =>
              Views.using[T].apply { views =>
                (views.removeFromActiveView(to) *> views.addToPassiveView(to)).commit
              }
            case false =>
              Views.using[T].apply { views =>
                views.removeFromActiveView(to).commit
              }
          })
          config <- getConfig.toManaged_
        } yield (keepInPassive, end, config)
      )
      .catchAll(
        _ =>
          ZStream
            .fromEffect(log.warn(s"Not running active protocol as adding to active view failed for $to")) *> ZStream.empty
      )
      .flatMap {
        case (keepInPassive, end, config) =>
          stream
            .mapM { raw =>
              TaggedCodec
                .read[ActiveProtocol[T]](raw)
                .mapError(e => zio.membership.DeserializationError(e.msg))
                .tap(msg => log.debug(s"receiveActiveProtocol: $to -> $msg"))
                .flatMap {
                  case msg: Disconnect[T] =>
                    keepInPassive.set(msg.alive).as((false, None))
                  case msg: ForwardJoin[T] =>
                    Views.using[T].apply { views =>
                      TRandom.using { tRandom =>
                        val accept =
                          log.info(s"Joining ${msg.originalSender} via ForwardJoin.") *>
                            sendInitial(msg.originalSender, InitialMessage.ForwardJoinReply(views.myself))

                        val process = views.activeViewSize.map[(Int, Option[TimeToLive])]((_, msg.ttl.step)).flatMap {
                          case (i, _) if i <= 1 =>
                            STM.succeed(accept)
                          case (_, None) =>
                            STM.succeed(accept)
                          case (_, Some(ttl)) =>
                            for {
                              list <- views.activeView
                                       .map(_.filterNot(n => n == msg.sender || n == msg.originalSender).toList)
                              _ <- if (ttl.count == config.prwl) views.addToPassiveView(msg.originalSender)
                                  else STM.unit
                              forward = msg.copy(sender = views.myself, ttl = ttl)
                            } yield {
                              def go(candidates: List[T]): UIO[Unit] =
                                tRandom
                                  .selectOne(candidates)
                                  .commit
                                  .flatMap(
                                    _.fold(ZIO.unit)(
                                      c => views.send(c, forward).orElse(go(candidates.filterNot(_ == c)))
                                    )
                                  )
                              go(list)
                            }
                        }
                        process.commit.flatten.as((true, None))
                      }
                    }
                  case msg: Shuffle[T] =>
                    Views.using[T].apply { views =>
                      TRandom.using { tRandom =>
                        views.activeViewSize
                          .map[(Int, Option[TimeToLive])]((_, msg.ttl.step))
                          .flatMap {
                            case (0, _) | (_, None) =>
                              for {
                                passive   <- views.passiveView
                                sentNodes = msg.activeNodes ++ msg.passiveNodes
                                replyNodes <- tRandom.selectN(
                                               passive.filterNot(_ == msg.originalSender).toList,
                                               config.shuffleNActive + config.shuffleNPassive
                                             )
                                _ <- views.addAllToPassiveView(sentNodes)
                              } yield sendInitial(
                                msg.originalSender,
                                InitialMessage.ShuffleReply(replyNodes, sentNodes)
                              ).ignore
                            case (_, Some(ttl)) =>
                              for {
                                active <- views.activeView.map(
                                           _.filterNot(n => n == msg.sender || n == msg.originalSender).toList
                                         )
                                forward = msg.copy(sender = views.myself, ttl = ttl)
                              } yield {
                                def go(candidates: List[T]): UIO[Unit] =
                                  tRandom
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
                    }
                  case m: UserMessage => ZIO.succeed((true, Some(m.msg)))
                }
            }
            .mapError(Right(_))
            .merge(ZStream.fromEffect(end.await).mapError(Left(_)))
      }
      .takeWhile(_._1)
      .collect {
        case (_, Some(msg)) => msg
      }
      .catchAll {
        case Left(_)  => ZStream.empty
        case Right(e) => ZStream.fail(e)
      }
  }

}
