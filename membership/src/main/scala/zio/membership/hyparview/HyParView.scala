package zio.membership.hyparview

import zio._
import zio.stm._
import zio.membership.transport.{ ChunkConnection, Transport }
import zio.random.Random
import zio.macros.delegate._
import com.github.ghik.silencer.silent
import zio.console.Console
import zio.membership.Membership
import zio.membership.ByteCodec
import zio.duration._
import zio.clock.Clock

object HyParView {

  @silent("deprecated")
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
      r            <- ZManaged.environment[R]
      activeView0  <- TMap.empty[T, ChunkConnection].commit.toManaged_
      passiveView0 <- TSet.empty[T].commit.toManaged_
      pickRandom0 <- {
        for {
          seed    <- random.nextInt.toManaged_
          sRandom = new scala.util.Random(seed)
          ref     <- TRef.make(Stream.continually((i: Int) => sRandom.nextInt(i))).commit.toManaged_
        } yield (i: Int) => ref.modify(s => (s.head(i), s.tail))
      }
      env <- ZManaged.environment[Transport[T] with zio.console.Console] @@ enrichWith[Env[T]] {
              new Env[T] {
                val env = new Env.Service[T] {
                  override val myself      = localAddr
                  override val activeView  = activeView0
                  override val passiveView = passiveView0
                  override val pickRandom  = pickRandom0
                  override val cfg         = config
                }
              }
            }
      _ <- (for {
            active  <- activeView0.keys.map(_.size).commit
            passive <- passiveView0.size.commit
            _       <- console.putStrLn(s"${localAddr}: { active: $active, passive: $passive }")
          } yield ()).repeat(Schedule.spaced(1.second)).toManaged_.fork
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
        override val nodes    = activeView0.keys.commit

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
