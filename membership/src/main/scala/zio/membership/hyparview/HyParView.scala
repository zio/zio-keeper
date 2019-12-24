package zio.membership.hyparview

import zio._
import zio.membership.transport.Transport
import zio.random.Random
import zio.macros.delegate.syntax._
import zio.membership.Membership
import zio.membership.ByteCodec
import zio.duration._
import zio.clock.Clock
import zio.stream.ZStream
import zio.membership.Error
import zio.stream.Take
import zio.logging.Logging

object HyParView {

  def apply[R <: Transport[T] with Random with Logging[String] with Clock, T](
    localAddr: T,
    activeViewCapacity: Int,
    passiveViewCapacity: Int,
    arwl: Int,
    prwl: Int,
    shuffleNActive: Int,
    shuffleNPassive: Int,
    shuffleTTL: Int,
    neighborSchedule: Schedule[R, Int, Any],
    shuffleSchedule: Schedule[R, Int, Any],
    outboundMessagesBuffer: Int,
    userMessagesBuffer: Int,
    nWorkers: Int
  )(
    implicit
    ev1: Tagged[InitialProtocol[T]],
    ev2: Tagged[ActiveProtocol[T]],
    ev3: ByteCodec[JoinReply[T]]
  ): ZManaged[R, Error, Membership[T]] = {
    type R1 = Clock with Random with Transport[T] with Logging[String] with Cfg with Views[T]

    val config = Config(
      activeViewCapacity,
      passiveViewCapacity,
      arwl,
      prwl,
      shuffleNActive,
      shuffleNPassive,
      shuffleTTL
    )
    for {
      r            <- ZManaged.environment[R]
      initialQueue <- Queue.bounded[(T, InitialProtocol[T])](outboundMessagesBuffer).toManaged_
      sendInitial = (to: T, msg: InitialProtocol[T]) =>
        initialQueue
          .offer((to, msg))
          .catchSomeCause {
            case cause if cause.interrupted => ZIO.unit
          }
          .unit
      env <- ZManaged.environment[
              Clock with Random with Transport[T] with Logging[String]
            ] @@
              Cfg.withStaticConfig(config) @@
              Views.withViews(
                localAddr,
                activeViewCapacity,
                passiveViewCapacity
              )
      outgoing = InitialProtocol
        .send[
          R1,
          R1,
          Error,
          Error,
          T,
          Chunk[Byte]
        ](
          ZStream.fromQueue(initialQueue)
        ) {
          case (addr, send, receive) =>
            ActiveProtocol
              .receive(
                receive,
                addr,
                send,
                sendInitial
              )
              .orElse(ZStream.empty)
        }
      incoming = env.transport
        .bind(localAddr)
        .map(ZStream.managed(_).flatMap { c =>
          InitialProtocol
            .receive[
              R1,
              R1,
              Error,
              Error,
              T,
              Chunk[Byte]
            ](
              c.receive,
              c.send
            ) { (addr, receive) =>
              ActiveProtocol
                .receive(
                  receive,
                  addr,
                  c.send,
                  sendInitial
                )
                .orElse(ZStream.empty)
            }
            .orElse(ZStream.empty)
        })
      userMessages <- Queue.dropping[Take[Error, Chunk[Byte]]](userMessagesBuffer).toManaged_
      _ <- outgoing
            .merge(incoming)
            .flatMapPar(nWorkers)(identity)
            .into(userMessages)
            .provide(env)
            .toManaged_
            .fork
      _ <- periodic.doNeighbor[Nothing, T](sendInitial).repeat(neighborSchedule.provide(r)).provide(env).toManaged_.fork
      _ <- periodic.doShuffle[T].repeat(shuffleSchedule.provide(r)).provide(env).toManaged_.fork
      _ <- periodic.doReport[T].repeat(Schedule.spaced(1.second)).provide(env).toManaged_.fork
    } yield new Membership[T] {
      val membership = new Membership.Service[Any, T] {

        override val identity = ZIO.succeed(localAddr)

        override val nodes = env.views.activeView.commit

        override def join(node: T) =
          sendInitial(node, InitialProtocol.Join(env.views.myself))

        override def send[A: ByteCodec](to: T, payload: A) =
          for {
            chunk <- ByteCodec[A].toChunk(payload)
            _     <- env.views.send(to, ActiveProtocol.UserMessage(chunk)).ignore
          } yield ()

        override def broadcast[A: ByteCodec](payload: A) = ???

        override def receive[A: ByteCodec] =
          ZStream.fromQueue(userMessages).unTake.mapM(ByteCodec[A].fromChunk(_))
      }
    }
  }
}
