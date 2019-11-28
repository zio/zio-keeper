package zio.keeper.transport

import zio._
import zio.test._
import zio.test.Assertion._
import zio.clock.Clock
import zio.console.Console
import zio.duration._
import zio.keeper.TransportError
import zio.macros.delegate._
import zio.nio.SocketAddress
import zio.test.environment.{Live, TestClock, TestConsole}

object TransportSpec
  extends DefaultRunnableSpec({
    // todo: actually find free port
    val freePort = ZIO.succeed(8010)

    val withTransport = tcp.withTcpTransport(10.seconds, 10.seconds)

    val environment =
      for {
        test <- (ZIO.environment[TestClock with TestConsole] @@ withTransport)
        live <- (Live.live(ZIO.environment[Clock with Console]) @@ withTransport).flatMap(Live.make)
        env <- ZIO.succeed(live) @@ enrichWith(test)
      } yield env

    suite("TcpTransport")(
      testM("can send and receive messages") {
        checkM(Gen.listOf(Gen.anyByte)) { bytes =>
          val payload = Chunk.fromIterable(bytes)

          def readOne(addr: SocketAddress, startPromise: Promise[TransportError, Unit]) =
            for {
              q <- Queue.bounded[Chunk[Byte]](10)
              h = (out: ChannelOut) => {
                for {
                  data <- out.read
                  _ <- q.offer(data)
                } yield ()
              }.ignore
              p <- bind(addr)(h).use_(startPromise.succeed(()) *> q.take)
            } yield p
          environment >>> Live.live(for {
            port <- freePort
            addr <- SocketAddress.inetSocketAddress(port)
            startPromise <- Promise.make[TransportError, Unit]
            chunk <- readOne(addr, startPromise).fork
            _ <- startPromise.await
            _ <- connect(addr).use(_.send(payload).retry(Schedule.spaced(10.milliseconds)))
            result <- chunk.join
          } yield assert(result, equalTo(payload)))
        }
      }
        //      ,
//      testM("handles interrupts like a good boy") {
//        val payload = Chunk.single(Byte.MaxValue)
//
//        environment >>> Live.live(for {
//          latch <- Promise.make[Nothing, Unit]
//          port <- freePort.map(_ + 1)
//          addr <- SocketAddress.inetSocketAddress(port)
//          fiber <- bind(addr).take(2).tap(_ => latch.succeed(())).runDrain.fork
//          _ <- send(addr, payload).retry(Schedule.spaced(10.milliseconds))
//          result <- latch.await *> fiber.interrupt
//        } yield assert(result, isInterrupted))
//      }
    )
  })
