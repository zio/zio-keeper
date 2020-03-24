package zio.keeper.transport

import zio._
import zio.console.{Console, _}
import zio.duration._
import zio.keeper.TransportError.ExceptionWrapper
import zio.keeper.transport.Channel.Connection
import zio.logging.Logging
import zio.nio.core.SocketAddress
import zio.test.Assertion._
import zio.test._
import zio.test.environment.TestEnvironment

object TransportSpec extends DefaultRunnableSpec {

  private val tcpEnv =
    (TestEnvironment.live ++ Logging.ignore) >>> tcp.live(10.seconds, 10.seconds)

  private val udpEnv =
    (TestEnvironment.live ++ Logging.ignore) >>> udp.live(128)

  def bindAndWaitForValue(
    addr: SocketAddress,
    startServer: Promise[Nothing, SocketAddress],
    handler: Connection => UIO[Unit] = _ => ZIO.unit
  ) =
    for {
      q <- Queue.bounded[Chunk[Byte]](10)
      h = (out: Connection) => {
        for {
          _    <- handler(out)
          data <- out.read
          _    <- q.offer(data)
        } yield ()
      }.catchAll(ex => putStrLn("error in server: " + ex).provideLayer(Console.live))
      p <- bind(addr)(h).use { bind =>
            for {
              address <- bind.localAddress
              _       <- startServer.succeed(address)
              chunk   <- q.take
            } yield chunk
          }
    } yield p

  def spec =
    suite("transport")(
      suite("tcp")(
        testM("can send and receive messages") {
          checkM(Gen.listOf(Gen.anyByte)) { bytes =>
            val payload = Chunk.fromIterable(bytes)
            for {
              addr        <- SocketAddress.inetSocketAddress(0)
              startServer <- Promise.make[Nothing, SocketAddress]
              chunk       <- bindAndWaitForValue(addr, startServer).fork
              address     <- startServer.await
              _           <- connect(address).use(_.send(payload).retry(Schedule.spaced(10.milliseconds)))
              result      <- chunk.join
            } yield assert(result)(equalTo(payload))
          }
        },
        testM("we should be able to close the client connection") {
          for {
            addr    <- SocketAddress.inetSocketAddress(0)
            promise <- Promise.make[Nothing, Unit]
            result <- bind(addr)(channel => channel.close.ignore.repeat(Schedule.doUntilM(_ => promise.isDone))).use {
                       bind =>
                         for {
                           address <- bind.localAddress
                           res <- connect(address).use(client => client.read.ignore) *>
                                   connect(address).use(client => client.read).either <*
                                   promise.succeed(())
                         } yield res
                     }
          } yield (result match {
            case Right(_) =>
              assert(false)(equalTo(true))
            case Left(ex: ExceptionWrapper) =>
              assert(ex.throwable.getMessage)(equalTo("Connection reset by peer"))
            case Left(_) =>
              assert(false)(equalTo(true))
          })
        },
        testM("handles interrupts like a champ") {
          val payload = Chunk.single(Byte.MaxValue)
          for {
            startServer <- Promise.make[Nothing, SocketAddress]
            addr        <- SocketAddress.inetSocketAddress(0)
            fiber       <- bindAndWaitForValue(addr, startServer, _ => ZIO.never).fork
            address     <- startServer.await
            _           <- connect(address).use(_.send(payload)).retry(Schedule.spaced(10.milliseconds))
            result      <- fiber.interrupt
          } yield assert(result)(isInterrupted)
        }
      ).provideCustomLayer(tcpEnv),
      suite("udp")(
        testM("can send and receive messages") {
          checkM(Gen.listOf(Gen.anyByte)) { bytes =>
            val payload = Chunk.fromIterable(bytes)
            for {
              addr        <- SocketAddress.inetSocketAddress(0)
              startServer <- Promise.make[Nothing, SocketAddress]
              chunk       <- bindAndWaitForValue(addr, startServer).fork
              address     <- startServer.await
              _           <- connect(address).use(_.send(payload).retry(Schedule.spaced(10.milliseconds)))
              result      <- chunk.join
            } yield assert(result)(equalTo(payload))
          }
        }
      ).provideCustomLayer(udpEnv)
    )
}
