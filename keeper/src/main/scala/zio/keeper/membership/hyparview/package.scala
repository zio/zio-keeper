package zio.keeper.membership

import java.util.UUID

import zio.keeper.{ ByteCodec, Error, NodeAddress, SendError, SerializationError, TaggedCodec, TransportError }
import zio.keeper.SerializationError.{ DeserializationTypeError, SerializationTypeError }
import zio.keeper.membership.hyparview.InitialProtocol._
import zio.keeper.transport.{ ChunkConnection, Transport }
import zio.logging.Logging
import zio.logging.log
import zio.stm.{ STM, ZSTM }
import zio.stream.{ Stream, ZStream }
import zio.{ Chunk, Has, IO, Promise, Ref, UIO, URIO, ZIO, ZManaged, ZQueue }

package object hyparview {
  type Membership[A]   = Has[Membership.Service[A]]
  type PeerService     = Has[PeerService.Service]
  type HyParViewConfig = Has[HyParViewConfig.Service]
  type TRandom         = Has[TRandom.Service]
  type Views           = Has[Views.Service]

  type Enqueue[-A] = ZQueue[Any, Nothing, Any, Nothing, A, Any]

  def broadcast(data: Chunk[Byte]): ZIO[Membership[Chunk[Byte]], Error, Unit] =
    ZIO.accessM(_.get.broadcast(data))

  //  def events: ZStream[Membership[Chunk[Byte]], Error, MembershipEvent] =
  //    ZStream.accessStream(_.get.events)

  def localMember: ZIO[Membership[Chunk[Byte]], Nothing, NodeAddress] =
    ZIO.accessM(_.get.localMember)

  def nodes: ZIO[Membership[Chunk[Byte]], Nothing, Set[NodeAddress]] =
    ZIO.accessM(_.get.nodes)

  def receive: ZStream[Membership[Chunk[Byte]], Error, (NodeAddress, Chunk[Byte])] =
    ZStream.accessStream(_.get.receive)

  def send(data: Chunk[Byte], receipt: NodeAddress): ZIO[Membership[Chunk[Byte]], Error, Unit] =
    ZIO.accessM(_.get.send(data, receipt))

  val getConfig: URIO[HyParViewConfig, HyParViewConfig.Config] =
    URIO.accessM[HyParViewConfig](_.get.getConfig)

  val makeRandomUUID: UIO[UUID] = ZIO.effectTotal(UUID.randomUUID())

  private[hyparview] def readJoinReply[R, E >: DeserializationTypeError, T](
    stream: ZStream[R, E, Chunk[Byte]]
  )(
    implicit ev: ByteCodec[JoinReply]
  ): ZManaged[R, E, Option[(NodeAddress, ZStream[R, E, Chunk[Byte]])]] =
    stream.process.mapM { pull =>
      val continue =
        pull.foldM[R, E, Option[NodeAddress]](
          _.fold[ZIO[R, E, Option[NodeAddress]]](ZIO.succeed(None))(ZIO.fail(_)), { msg =>
            ByteCodec[JoinReply]
              .fromChunk(msg)
              .mapError(e => DeserializationTypeError(e.msg))
              .map {
                case JoinReply(addr) => Some(addr)
              }
          }
        )
      continue.map(_.map((_, ZStream.repeatEffectOption(pull))))
    }

  private[hyparview] def readNeighborReply[R, E >: DeserializationTypeError, A](
    stream: ZStream[R, E, Chunk[Byte]]
  ): ZManaged[R, E, Option[Stream[E, Chunk[Byte]]]] =
    stream.process.mapM { pull =>
      val continue =
        pull.foldM[R, E, Boolean](
          _.fold[ZIO[R, E, Boolean]](ZIO.succeed(false))(ZIO.fail(_)), { msg =>
            TaggedCodec
              .read[NeighborReply](msg)
              .mapError(e => DeserializationTypeError(e.msg))
              .map {
                case NeighborReply.Accept => true
                case NeighborReply.Reject => false
              }
          }
        )
      ZIO.environment[R].flatMap { env =>
        continue.map(if (_) Some(ZStream.repeatEffectOption(pull).provide(env)) else None)
      }
    }

  private[hyparview] def sendInitial(
    to: NodeAddress,
    msg: InitialMessage,
    allocate: ZManaged.Scope,
    connections: Enqueue[(NodeAddress, Chunk[Byte] => IO[TransportError, Unit], Stream[Error, Chunk[Byte]], UIO[_])]
  ): ZIO[Logging with Transport, Error, Unit] =
    log.debug(s"sendInitial $to -> $msg") *> (allocate {
      def openConnection(to: NodeAddress, msg: InitialProtocol) =
        for {
          con <- Transport.connect(to)
          msg <- TaggedCodec
                  .write[InitialProtocol](msg)
                  .mapError(e => SerializationTypeError(e.msg))
                  .toManaged_
          _ <- con.send(msg).toManaged_
        } yield con

      msg match {
        case m: ForwardJoinReply =>
          openConnection(to, m).map { con =>
            Some((to, con.send(_), con.receive))
          }
        case m: Join =>
          openConnection(to, m).flatMap { con =>
            readJoinReply(con.receive).map(_.map { case (addr, receive) => (addr, con.send(_), receive) })
          }
        case m: ShuffleReply =>
          openConnection(to, m).as(None)
      }
    }).foldCauseM(
      log.error(s"Failed sending initialMessage $msg to $to", _), {
        case (None, release) =>
          release.unit
        case (Some((addr, send, receive)), release) =>
          connections.offer((addr, send, receive, release)).unit
      }
    )

  def receiveInitialProtocol[R <: Views with Transport with Logging with HyParViewConfig, E >: Error](
    stream: ZStream[R, E, ChunkConnection],
    concurrentConnections: Int = 16
  ): ZStream[R, E, (NodeAddress, Chunk[Byte] => IO[TransportError, Unit], Stream[Error, Chunk[Byte]], UIO[_])] =
    ZStream.managed(ZManaged.scope).flatMap { allocate =>
      stream
        .mapMPar(concurrentConnections) { con =>
          allocate {
            con.receive.process
              .mapM[R, E, Option[(NodeAddress, Chunk[Byte] => IO[TransportError, Unit], Stream[Error, Chunk[Byte]])]] {
                pull =>
                  pull
                    .foldM(
                      _.fold[ZIO[R, E, Option[NodeAddress]]](ZIO.succeed(None))(ZIO.fail(_)), { raw =>
                        TaggedCodec
                          .read[InitialProtocol](raw)
                          .mapError(e => SerializationError.DeserializationTypeError(e.msg))
                          .tap(msg => log.debug(s"receiveInitialProtocol: $msg"))
                          .flatMap {
                            case msg: Neighbor =>
                              val accept = for {
                                reply <- TaggedCodec
                                          .write[NeighborReply](NeighborReply.Accept)
                                          .mapError(e => SerializationTypeError(e.msg))
                                _ <- log.debug(s"Accepting neighborhood request from ${msg.sender}")
                                _ <- con.send(reply)
                              } yield Some(msg.sender)

                              val reject = for {
                                reply <- TaggedCodec
                                          .write[NeighborReply](NeighborReply.Reject)
                                          .mapError(e => SerializationTypeError(e.msg))
                                _ <- log.debug(s"Rejecting neighborhood request from ${msg.sender}")
                                _ <- con.send(reply)
                              } yield None

                              if (msg.isHighPriority) {
                                accept
                              } else {
                                ZSTM.atomically {
                                  for {
                                    task <- ZSTM.ifM(Views.isActiveViewFull)(
                                             Views
                                               .addToPassiveView(msg.sender)
                                               .as(
                                                 reject
                                               ),
                                             STM.succeed(accept)
                                           )
                                  } yield task
                                }.flatten
                              }
                            case msg: Join =>
                              for {
                                others    <- Views.activeView.map(_.filterNot(_ == msg.sender)).commit
                                localAddr <- Views.myself.commit
                                config    <- getConfig
                                _ <- ZIO
                                      .foreachPar_(others)(
                                        node =>
                                          Views
                                            .send(
                                              node,
                                              ActiveProtocol
                                                .ForwardJoin(localAddr, msg.sender, TimeToLive(config.arwl))
                                            )
                                      )
                                reply <- ByteCodec
                                          .decode(JoinReply(localAddr))
                                          .mapError(e => SerializationTypeError(e.msg))
                                _ <- con.send(reply)
                              } yield Some(msg.sender)
                            case msg: ForwardJoinReply =>
                              // nothing to do here, we just continue to the next protocol
                              ZIO.succeed(Some(msg.sender))
                            case msg: ShuffleReply =>
                              Views
                                .addShuffledNodes(msg.sentOriginally.toSet, msg.passiveNodes.toSet)
                                .commit
                                .as(None)
                          }
                      }
                    )
                    .map(_.map((_, con.send, ZStream.repeatEffectOption(pull))))
              }
          }.foldCauseM(
            log.error("Failure while running initial protocol", _).as(None), {
              case (None, release)                        => release.as(None)
              case (Some((addr, send, receive)), release) => ZIO.succeed(Some((addr, send, receive, release)))
            }
          )
        }
        .collect {
          case Some(x) => x
        }
    }

  val neighborProtocol: ZStream[
    Views with Logging with Transport with Views with TRandom,
    Nothing,
    (NodeAddress, Chunk[Byte] => ZIO[Any, TransportError, Unit], Stream[Error, Chunk[Byte]], UIO[_])
  ] =
    ZStream
      .managed(ZManaged.scope)
      .flatMap { preallocate =>
        ZStream
          .repeatEffect[Views with TRandom, Nothing, (NodeAddress, Neighbor)] {
            ZSTM.atomically {
              for {
                _              <- Views.isActiveViewFull.retryWhile(identity)
                activeViewSize <- Views.activeViewSize
                localAddr      <- Views.myself
                nodeOpt        <- Views.passiveView.flatMap(xs => TRandom.selectOne(xs.toList))
                node           <- nodeOpt.fold[STM[Nothing, NodeAddress]](STM.retry)(STM.succeed(_))
              } yield (node, Neighbor(localAddr, activeViewSize <= 0))
            }
          }
          .mapM {
            case (node, msg) =>
              preallocate {
                for {
                  _   <- log.debug(s"Running neighbor protocol with remote $node").toManaged_
                  con <- Transport.connect(node)
                  msg <- TaggedCodec
                          .write[InitialProtocol](msg)
                          .mapError(e => SerializationTypeError(e.msg))
                          .toManaged_
                  _    <- con.send(msg).toManaged_
                  cont <- readNeighborReply(con.receive)
                } yield cont.map((node, con.send(_), _))
              }.foldCauseM(
                e =>
                  for {
                    _ <- log.error(s"Failed neighbor protocol with remote $node", e)
                    _ <- Views.removeFromPassiveView(node).commit
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

  def runActiveProtocol[R <: Views with HyParViewConfig with Logging with TRandom, E >: Error](
    remote: NodeAddress,
    reply: Chunk[Byte] => IO[TransportError, Unit],
    sendInitial: (NodeAddress, InitialMessage) => IO[E, Unit]
  )(
    stream: ZStream[R, E, Chunk[Byte]]
  ): ZStream[R, E, (NodeAddress, ActiveProtocol.PlumTreeProtocol)] = {
    import ActiveProtocol._
    ZStream
      .managed(
        for {
          env <- ZManaged.environment[R]
          end <- Promise.make[Unit, Nothing].toManaged_
          keepInPassive <- {
            for {
              _ <- Views
                    .addToActiveView(
                      remote,
                      msg =>
                        (for {
                          chunk <- TaggedCodec.write[ActiveProtocol](msg).mapError(SendError.SerializationFailed)
                          _     <- reply(chunk).mapError(SendError.TransportFailed)
                          _     <- log.error(s"sendActiveProtocol: $remote -> $msg")
                        } yield ())
                          .tapCause(log.error(s"Failed sending message $msg to $remote", _))
                          .provide(env),
                      end.fail(()).unit
                    )
                    .commit
              ref <- Ref.make(false)
            } yield ref
          }.toManaged(_.get.flatMap {
            case true =>
              (Views.removeFromActiveView(remote) *> Views.addToPassiveView(remote)).commit
            case false =>
              Views.removeFromActiveView(remote).commit
          })
          config <- getConfig.toManaged_
        } yield (keepInPassive, end, config)
      )
      .catchAll(
        _ =>
          ZStream
            .fromEffect(log.warn(s"Not running active protocol as adding to active view failed for $remote")) *> ZStream.empty
      )
      .flatMap {
        case (keepInPassive, end, config) =>
          stream
            .mapM { raw =>
              TaggedCodec
                .read[ActiveProtocol](raw)
                .mapError(e => DeserializationTypeError(e.msg))
                .tap(msg => log.debug(s"receiveActiveProtocol: $remote -> $msg"))
                .flatMap {
                  case msg: Disconnect =>
                    keepInPassive.set(msg.alive).as((false, None))
                  case msg: ForwardJoin =>
                    val accept =
                      log.info(s"Joining ${msg.originalSender} via ForwardJoin.") *>
                        Views.myself.commit
                          .flatMap(localAddr => sendInitial(msg.originalSender, ForwardJoinReply(localAddr)))

                    val process = Views.activeViewSize.map[(Int, Option[TimeToLive])]((_, msg.ttl.step)).flatMap {
                      case (i, _) if i <= 1 =>
                        STM.succeed(accept)
                      case (_, None) =>
                        STM.succeed(accept)
                      case (_, Some(ttl)) =>
                        for {
                          list <- Views.activeView
                                   .map(_.filterNot(n => n == msg.sender || n == msg.originalSender).toList)
                          _ <- if (ttl.count == config.prwl) Views.addToPassiveView(msg.originalSender)
                              else STM.unit
                          localAddr <- Views.myself
                          forward   = msg.copy(sender = localAddr, ttl = ttl)
                        } yield {
                          def go(candidates: List[NodeAddress]): URIO[TRandom with Views, Unit] =
                            TRandom
                              .selectOne(candidates)
                              .commit
                              .flatMap(
                                _.fold[URIO[TRandom with Views, Unit]](ZIO.unit)(
                                  c => Views.send(c, forward).orElse(go(candidates.filterNot(_ == c)))
                                )
                              )
                          go(list)
                        }
                    }
                    process.commit.flatten.as((true, None))
                  case msg: Shuffle =>
                    Views.activeViewSize
                      .map[(Int, Option[TimeToLive])]((_, msg.ttl.step))
                      .flatMap {
                        case (0, _) | (_, None) =>
                          for {
                            passive   <- Views.passiveView
                            sentNodes = msg.activeNodes ++ msg.passiveNodes
                            replyNodes <- TRandom.selectN(
                                           passive.filterNot(_ == msg.originalSender).toList,
                                           config.shuffleNActive + config.shuffleNPassive
                                         )
                            _ <- Views.addAllToPassiveView(sentNodes)
                          } yield sendInitial(
                            msg.originalSender,
                            ShuffleReply(replyNodes, sentNodes)
                          ).ignore
                        case (_, Some(ttl)) =>
                          for {
                            active <- Views.activeView.map(
                                       _.filterNot(n => n == msg.sender || n == msg.originalSender).toList
                                     )
                            localAddr <- Views.myself
                            forward   = msg.copy(sender = localAddr, ttl = ttl)
                          } yield {
                            def go(candidates: List[NodeAddress]): URIO[TRandom with Views, Unit] =
                              TRandom
                                .selectOne(candidates)
                                .commit
                                .flatMap(
                                  _.fold[URIO[TRandom with Views, Unit]](ZIO.unit)(
                                    c => Views.send(c, forward).orElse(go(candidates.filterNot(_ == c)))
                                  )
                                )
                            go(active)
                          }
                      }
                      .commit
                      .flatten
                      .as((true, None))
                  case m: PlumTreeProtocol =>
                    // message is handled by next layer
                    ZIO.succeed((true, Some(remote -> m)))
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
