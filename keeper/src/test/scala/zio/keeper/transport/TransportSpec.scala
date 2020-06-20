package zio.keeper.transport

import zio._
import zio.console.{ Console, _ }
import zio.duration._
import zio.keeper.TransportError
import zio.logging.Logging
import zio.nio.core.SocketAddress
import zio.test.Assertion._
import zio.test._
import zio.test.environment.TestEnvironment

object TransportSpec extends DefaultRunnableSpec {

  private val udpEnv =
    (TestEnvironment.live ++ Logging.ignore) >>> udp.live(128)

  def bindAndWaitForValue(
    addr: SocketAddress,
    startServer: Promise[Nothing, SocketAddress],
    handler: Channel => UIO[Unit] = _ => ZIO.unit
  ): ZIO[ConnectionLessTransport, TransportError, Chunk[Byte]] =
    for {
      q <- Queue.bounded[Chunk[Byte]](10)
      h = (out: Channel) => {
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
