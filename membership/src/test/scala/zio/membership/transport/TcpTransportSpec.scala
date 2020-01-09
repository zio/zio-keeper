package zio.membership.transport

import zio._
import zio.test._
import zio.test.Assertion._
import zio.clock.Clock
import zio.duration._
import zio.macros.delegate._
import zio.macros.delegate.syntax._
import zio.nio.SocketAddress
import zio.test.environment.TestClock
import zio.test.environment.Live

object TransportSpec
    extends DefaultRunnableSpec({
      // todo: actually find free port
      val freePort = ZIO.succeed(8010)

      val withTransport = tcp.withTcpTransport(10, 10.seconds, 10.seconds)

      val environment =
        for {
          test <- (ZIO.environment[TestClock] @@ withTransport)
          live <- (Live.live(ZIO.environment[Clock]) @@ withTransport).flatMap(Live.make(_))
          env  <- ZIO.succeed(live) @@ enrichWith(test)
        } yield env

      suite("TcpTransport")(
        testM("can send and receive messages") {
          checkM(Gen.listOf(Gen.anyByte)) { bytes =>
            val payload = Chunk.fromIterable(bytes)

            def readOne(addr: SocketAddress) =
              bind(addr).take(1).runCollect.map(_.head)

            environment >>> Live.live(for {
              port   <- freePort
              addr   <- SocketAddress.inetSocketAddress(port)
              chunk  <- readOne(addr).fork
              _      <- send(addr, payload).retry(Schedule.spaced(10.milliseconds))
              result <- chunk.join
            } yield assert(result, equalTo(payload)))
          }
        },
        testM("handles interrupts like a good boy") {
          val payload = Chunk.single(Byte.MaxValue)

          environment >>> Live.live(for {
            latch  <- Promise.make[Nothing, Unit]
            port   <- freePort.map(_ + 1)
            addr   <- SocketAddress.inetSocketAddress(port)
            fiber  <- bind(addr).take(2).tap(_ => latch.succeed(())).runDrain.fork
            _      <- send(addr, payload).retry(Schedule.spaced(10.milliseconds))
            result <- latch.await *> fiber.interrupt
          } yield assert(result, isInterrupted))
        }
      )
    })
