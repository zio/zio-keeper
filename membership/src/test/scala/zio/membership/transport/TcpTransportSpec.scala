package zio.membership.transport

import zio._
import zio.test._
import zio.test.Assertion._
import zio.clock.Clock
import zio.duration._
import zio.macros.delegate._
import zio.nio.SocketAddress
import zio.test.environment.TestClock
import zio.test.environment.Live
import zio.test.environment.TestEnvironment
import zio.test.TestAspect._

object TransportSpec
    extends DefaultRunnableSpec({
      // todo: actually find free port
      def freePort = ZIO.succeed(8010)

      val connectDelay = 300.milliseconds

      val withTransport = tcp.withTcpTransport(10, 10.seconds, 10.seconds)

      def environment: ZIO[TestEnvironment, Nothing, TestClock with Transport with Live[Clock with Transport]] =
        for {
          clock  <- Live.live(ZIO.environment[Clock])
          tClock <- ZIO.environment[TestClock]
          live   <- (ZIO.succeed(clock) @@ withTransport).flatMap(Live.make(_))
          test   <- (ZIO.succeed(tClock) @@ withTransport)
          env    <- ZIO.succeed(live) @@ enrichWith(test)
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
              _      <- send(addr, payload).delay(connectDelay)
              result <- chunk.join
            } yield assert(result, equalTo(payload)))
          }
        } @@ flaky @@ timeout(120.seconds) // this one is slow
      )
    })
