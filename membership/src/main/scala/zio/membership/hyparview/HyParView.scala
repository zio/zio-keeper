package zio.membership.hyparview

import zio._
import zio.membership.transport.Transport
import zio.membership.log
import zio.random.Random
import zio.macros.delegate._
import zio.macros.delegate.syntax._
import zio.membership.{ ByteCodec, Error, Membership, SendError, TransportError }
import zio.duration._
import zio.clock.Clock
import zio.stream.{ Stream, Take, ZStream }
import zio.logging.Logging

object HyParView {

  def withHyParView[R <: Transport[T] with Random with Logging[String] with Clock with HyParViewConfig, T](
    localAddr: T,
    shuffleSchedule: Schedule[R, ViewState, Any]
  )(
    implicit
    ev1: TaggedCodec[InitialProtocol[T]],
    ev2: TaggedCodec[ActiveProtocol[T]],
    ev3: ByteCodec[JoinReply[T]]
  ) =
    enrichWithManaged[Membership[T]](
      apply(localAddr, shuffleSchedule)
    )

  def apply[R <: Transport[T] with TRandom with Logging[String] with Clock with HyParViewConfig, T](
    localAddr: T,
    shuffleSchedule: Schedule[R, ViewState, Any]
  )(
    implicit
    ev1: TaggedCodec[InitialProtocol[T]],
    ev2: TaggedCodec[ActiveProtocol[T]],
    ev3: ByteCodec[JoinReply[T]]
  ): ZManaged[R, Error, Membership[T]] = {
    type R1 = Clock with TRandom with Transport[T] with Logging[String] with HyParViewConfig with Views[T]
    for {
      r   <- ZManaged.environment[R]
      cfg <- getConfig.toManaged_
      _ <- log
            .info(s"Starting HyParView on $localAddr with configuration:\n${cfg.prettyPrint}")
            .toManaged(_ => log.info("Shut down HyParView"))
      scope <- ScopeIO.make
      connections <- Queue
                      .bounded[(T, Chunk[Byte] => IO[TransportError, Unit], Stream[Error, Chunk[Byte]], UIO[_])](
                        cfg.connectionBuffer
                      )
                      .toManaged(_.shutdown)
      userMessages <- Queue.dropping[Take[Error, Chunk[Byte]]](cfg.userMessagesBuffer).toManaged(_.shutdown)
      env <- {
        ZManaged.environment[
          Clock with TRandom with Transport[T] with Logging[String] with HyParViewConfig
        ]
      } @@ {
        Views.withViews(
          localAddr,
          cfg.activeViewCapacity,
          cfg.passiveViewCapacity
        )
      }
      sendInitial0 = (to: T, msg: InitialMessage[T]) => sendInitial[T](to, msg, scope, connections).provide(r)
      _ <- receiveInitialProtocol[R1, Error, T](env.transport.bind(localAddr), cfg.concurrentIncomingConnections)
            .merge(ZStream.fromQueue(connections))
            .merge(neighborProtocol.scheduleElements(Schedule.spaced(2.seconds)))
            .flatMapParSwitch(cfg.activeViewCapacity) {
              case (addr, send, receive, release) =>
                runActiveProtocol(receive, addr, send, sendInitial0).ensuring(release)
            }
            .into(userMessages)
            .provide(env)
            .toManaged_
            .fork
      _ <- periodic
            .doShuffle[T]
            .repeat(shuffleSchedule.provide(r))
            .provide(env)
            .toManaged_
            .fork
      _ <- periodic.doReport[T].repeat(Schedule.spaced(2.seconds)).provide(env).toManaged_.fork
    } yield new Membership[T] {
      val membership = new Membership.Service[Any, T] {

        override val identity = ZIO.succeed(localAddr)

        override val nodes = env.views.activeView.commit

        override def join(node: T) =
          sendInitial0(node, InitialMessage.Join(env.views.myself))

        override def send[A: ByteCodec](to: T, payload: A) =
          for {
            chunk <- ByteCodec[A].toChunk(payload).mapError(SendError.SerializationFailed)
            _     <- env.views.send(to, ActiveProtocol.UserMessage(chunk))
          } yield ()

        override def broadcast[A: ByteCodec](payload: A) = ???

        override def receive[A: ByteCodec] =
          ZStream.fromQueue(userMessages).unTake.mapM(ByteCodec[A].fromChunk(_))
      }
    }
  }
}
