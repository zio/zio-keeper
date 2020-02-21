package zio.keeper.transport

import zio._
import zio.clock.Clock
import zio.console._
import zio.logging.Logging
import zio.logging.slf4j.Slf4jLogger
import zio.macros.delegate._
import zio.macros.delegate.syntax._
import zio.nio.SocketAddress
import zio.test.environment.Live

object TransportUtil {

  def transportEnvironment(transport: ZIO[Clock with Logging[String], Nothing, Transport]) = {
    val transportEnv =
      loggingEnv >>> ZIO.environment[ZEnv with Logging[String]] @@ enrichWithM(transport)

    val liveEnv =
      (Live.live(ZIO.environment[ZEnv]) @@ enrichWithM(transportEnv))
        .flatMap(Live.make)

    for {
      env <- transportEnv
      r   <- liveEnv @@ enrichWith(env)
    } yield r
  }

  def bindAndWaitForValue(
    addr: SocketAddress,
    startPromise: Promise[Nothing, Unit],
    handler: ChannelOut => UIO[Unit] = _ => ZIO.unit
  ) =
    for {
      q <- Queue.bounded[Chunk[Byte]](10)
      h = (out: ChannelOut) => {
        for {
          _    <- handler(out)
          _    <- UIO.effectTotal(println("reading data"))
          data <- out.read
          _    <- UIO.effectTotal(println(s"this is the data $data"))
          _    <- q.offer(data)
        } yield ()
      }.catchAll(ex => putStrLn("error in server: " + ex).provide(Console.Live))
      p <- bind(addr)(h).use_(startPromise.succeed(()) *> q.take)
    } yield p

  private val loggingEnv = ZIO.environment[ZEnv] @@ enrichWith[Logging[String]](
    new Slf4jLogger.Live {

      override def formatMessage(msg: String): ZIO[Any, Nothing, String] =
        UIO.succeed(msg)
    }
  )
}
