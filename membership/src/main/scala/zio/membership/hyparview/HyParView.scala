package zio.membership.hyparview

import zio._
import zio.membership.transport.Transport
import zio.random.Random
import zio.macros.delegate._
import zio.console.Console
import zio.membership.Membership
import zio.membership.ByteCodec
import zio.duration._
import zio.clock.Clock

object HyParView {

  def apply[R <: Transport[T] with Random with Console with Clock, T](
    localAddr: T,
    activeViewCapactiy: Int,
    passiveViewCapacity: Int,
    arwl: Int,
    prwl: Int,
    shuffleNActive: Int,
    shuffleNPassive: Int,
    shuffleTTL: Int,
    neighborSchedule: Schedule[R, Int, Any],
    shuffleSchedule: Schedule[R, Int, Any]
  )(
    implicit
    ev1: Tagged[InitialProtocol[T]],
    ev2: Tagged[Protocol[T]]
  ): ZManaged[R, Nothing, Membership[T]] = {
    val config = Config(
      activeViewCapactiy,
      passiveViewCapacity,
      arwl,
      prwl,
      shuffleNActive,
      shuffleNPassive,
      shuffleTTL
    )
    for {
      r <- ZManaged.environment[R]
      env <- ZManaged.environment[Clock with Random with Transport[T] with zio.console.Console] @@ Env.withEnv(
              localAddr,
              config
            )
      _ <- report[T].repeat(Schedule.spaced(1.second)).provide(env).toManaged_.fork
      _ <- env.transport
            .bind(localAddr)
            .foreach(InitialProtocol.handleInitialProtocol(_).fork)
            .provide(env)
            .toManaged_
            .fork
      _ <- periodic.doNeighbor.repeat(neighborSchedule.provide(r)).provide(env).toManaged_.fork
      _ <- periodic.doShuffle.repeat(shuffleSchedule.provide(r)).provide(env).toManaged_.fork
    } yield new Membership[T] {
      val membership = new Membership.Service[Any, T] {
        override val identity = ZIO.succeed(localAddr)
        override val nodes    = env.env.activeView.keys.commit

        override def connect(to: T) =
          for {
            con <- env.transport.connect(to)
            msg <- Tagged.write[InitialProtocol[T]](InitialProtocol.Join(localAddr))
            _   <- con.send(msg)
            _   <- addConnection(to, con).fork.provide(env)
          } yield ()

        override def send[A: ByteCodec](to: T, payload: A) = ???
        override def broadcast[A: ByteCodec](payload: A)   = ???
        override def receive[A: ByteCodec]                 = ???
      }
    }
  }
}
