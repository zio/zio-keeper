package zio.membership.hyparview

import zio._
import zio.membership.transport.Transport
import zio.membership.{ ByteCodec, Error, Membership, SendError, TransportError }
import zio.duration._
import zio.clock.Clock
import zio.stream.{ Stream, Take, ZStream }
import zio.logging.Logging
import zio.membership.transport

object HyParView {

  def live[T: Tagged](
    localAddr: T,
    shuffleSchedule: Schedule[Transport[T] with TRandom with Logging with Clock with HyParViewConfig, ViewState, Any]
  )(
    implicit
    ev1: TaggedCodec[InitialProtocol[T]],
    ev2: ByteCodec[JoinReply[T]],
    ev3: TaggedCodec[ActiveProtocol[T]]
  ): ZLayer[HyParViewConfig with Logging with Transport[T] with TRandom with Clock, Error, Membership[T]] =
    ZLayer.fromFunctionManaged { env =>
      for {
        views <- Managed.succeed(Views.fromConfig(localAddr))
        cfg   <- getConfig.toManaged_.provide(env)
        _ <- logging
              .logInfo(s"Starting HyParView on $localAddr with configuration:\n${cfg.prettyPrint}")
              .toManaged(_ => logging.logInfo("Shut down HyParView"))
              .provide(env)
        scope <- ScopeIO.make
        connections <- Queue
                        .bounded[(T, Chunk[Byte] => IO[TransportError, Unit], Stream[Error, Chunk[Byte]], UIO[_])](
                          cfg.connectionBuffer
                        )
                        .toManaged(_.shutdown)
        userMessages <- Queue.dropping[Take[Error, Chunk[Byte]]](cfg.userMessagesBuffer).toManaged(_.shutdown)
        sendInitial0 = (to: T, msg: InitialMessage[T]) => sendInitial[T](to, msg, scope, connections).provide(env)
        _ <- receiveInitialProtocol[Clock with TRandom with Transport[T] with Logging with HyParViewConfig with Views[
              T
            ], Error, T](transport.bind(localAddr), cfg.concurrentIncomingConnections)
              .merge(ZStream.fromQueue(connections))
              .merge(neighborProtocol.scheduleElements(Schedule.spaced(2.seconds)))
              .flatMapParSwitch(cfg.activeViewCapacity) {
                case (addr, send, receive, release) =>
                  runActiveProtocol[Views[T] with HyParViewConfig with Logging with TRandom, Error, T](
                    receive,
                    addr,
                    send,
                    sendInitial0
                  ).ensuring(release)
              }
              .into(userMessages)
              .toManaged_
              .provideSomeLayer[HyParViewConfig with Logging with Transport[T] with TRandom with Clock](views)
              .provide(env)
              .fork
        _ <- periodic
              .doShuffle[T]
              .repeat(shuffleSchedule)
              .toManaged_
              .provideSomeLayer[HyParViewConfig with Logging with Transport[T] with TRandom with Clock](views)
              .provide(env)
              .fork
        _ <- periodic
              .doReport[T]
              .repeat(Schedule.spaced(2.seconds))
              .toManaged_
              .provideSomeLayer[HyParViewConfig with Logging with Transport[T] with TRandom with Clock](views)
              .provide(env)
              .fork
      } yield new Membership.Service[T] {

        override val identity = ZIO.succeed(localAddr)

        override val nodes = Views
          .using[T]
          .apply(_.activeView.commit)
          .provideSomeLayer[HyParViewConfig with Logging with Transport[T] with TRandom with Clock](views)
          .provide(env)

        override def join(node: T): IO[Error, Unit] =
          Views
            .using[T]
            .apply(s => UIO.succeed(s.myself))
            .flatMap { my =>
              sendInitial0(node, InitialMessage.Join(my))
            }
            .provideSomeLayer[HyParViewConfig with Logging with Transport[T] with TRandom with Clock](views)
            .provide(env)

        override def send[A: ByteCodec](to: T, payload: A) =
          (for {
            chunk <- ByteCodec[A].toChunk(payload).mapError(SendError.SerializationFailed)
            _     <- Views.using[T].apply(_.send(to, ActiveProtocol.UserMessage(chunk)))
          } yield ())
            .provideSomeLayer[HyParViewConfig with Logging with Transport[T] with TRandom with Clock](views)
            .provide(env)

        override def broadcast[A: ByteCodec](payload: A) = ???

        override def receive[A: ByteCodec] =
          ZStream.fromQueue(userMessages).unTake.mapM(ByteCodec[A].fromChunk(_))
      }
    }
}
