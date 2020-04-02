package zio.membership.transport

import zio._
import zio.test._
import zio.test.Assertion._
import zio.clock.Clock
import zio.duration._
import zio.test.TestAspect._
import zio.logging.Logging
import zio.membership.KeeperSpec
import zio.stream.ZStream

object TcpTransportSpec extends KeeperSpec {

  val spec = {
    val addr = Address("localhost", 8081)

    val environment =
      ((Clock.live ++ Logging.ignore) >>> tcp.make(10, 10.seconds, 10.seconds)) ++ Clock.live

    (suite("TcpTransport")(
      testM("can send and receive messages") {
        checkM(Gen.listOf(Gen.anyByte)) { bytes =>
          val payload = Chunk.fromIterable(bytes)

          for {
            chunk <- Transport
                      .bind(addr)
                      .flatMap(c => c.receive.take(1).ensuring(c.close))
                      .take(1)
                      .runHead
                      .fork
            _      <- Transport.send(addr, payload).retry(Schedule.spaced(10.milliseconds))
            result <- chunk.join
          } yield assert(result)(isSome(equalTo(payload)))
        }
      },
      testM("handles interrupts") {
        val payload = Chunk.single(Byte.MaxValue)

        for {
          latch <- Promise.make[Nothing, Unit]
          fiber <- Transport
                    .bind(addr)
                    .flatMap(c => c.receive.take(1).tap(_ => latch.succeed(())).ensuring(c.close))
                    .take(2)
                    .runDrain
                    .fork
          _      <- Transport.send(addr, payload).retry(Schedule.spaced(10.milliseconds))
          result <- latch.await *> fiber.interrupt
        } yield assert(result)(isInterrupted)
      },
      testM("respects max connections") {
        for {
          ref    <- Ref.make(0)
          latch  <- Promise.make[Nothing, Unit]
          latch0 <- Promise.make[Nothing, Unit]
          connect = Transport
            .connect(addr)
            .use_ {
              ref.update(_ + 1) *> ZIO.never
            }
            .race(latch.await)
            .fork

          f1 <- Transport
                 .bind(addr)
                 .flatMapPar(20) { con =>
                   ZStream.fromEffect(latch0.succeed(())) *>
                     con.receive.take(1).ensuring(con.close)
                 }
                 .runDrain
                 .race(latch.await)
                 .fork
          f2     <- ZIO.collectAll(List.fill(10)(connect))
          _      <- latch0.await
          _      <- ZIO.sleep(200.millis)
          result <- ref.get
          _      <- latch.succeed(())
          _      <- ZIO.collectAll((f1 :: f2).map(_.await))
        } yield assert(result)(equalTo(10))
      }
    ) @@ sequential).provideCustomLayer(environment)
  }
}
