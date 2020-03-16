package zio.membership.transport

import zio._
import zio.test._
import zio.test.Assertion._
import zio.clock.Clock
import zio.duration._
import zio.stream.ZStream
import zio.test.TestAspect._
import zio.logging.Logging
import zio.membership.transport.tcp.Tcp

object TransportSpec extends DefaultRunnableSpec {
  val addr = Address("localhost", 8081)

  val environment =
    ((Clock.live ++ Logging.ignore) >>> Tcp.live(10, 10.seconds, 10.seconds)) ++ Clock.live

  def spec =
    (suite("TcpTransport")(
      testM("can send and receive messages") {
        checkM(Gen.listOf(Gen.anyByte)) { bytes =>
          val payload = Chunk.fromIterable(bytes)
          for {
            trans <- ZIO.environment[Transport[Address]].map(_.get)
            chunk <- trans
                      .bind(addr)
                      .flatMap(c => ZStream.managed(c).flatMap(_.receive.take(1)))
                      .take(1)
                      .runHead
                      .fork
            _      <- trans.send(addr, payload).retry(Schedule.spaced(10.milliseconds))
            result <- chunk.join
          } yield assert(result)(isSome(equalTo(payload)))
        }
      },
      testM("handles interrupts") {
        val payload = Chunk.single(Byte.MaxValue)
        for {
          trans <- ZIO.environment[Transport[Address]].map(_.get)
          latch <- Promise.make[Nothing, Unit]
          fiber <- trans
                    .bind(addr)
                    .flatMap(c => ZStream.managed(c).flatMap(_.receive.take(1).tap(_ => latch.succeed(()))))
                    .take(2)
                    .runDrain
                    .fork
          _      <- trans.send(addr, payload).retry(Schedule.spaced(10.milliseconds))
          result <- latch.await *> fiber.interrupt
        } yield assert(result)(isInterrupted)
      },
      testM("can receive messages after bind stream is closed") {
        checkM(Gen.listOf(Gen.anyByte)) { bytes =>
          val payload = Chunk.fromIterable(bytes)
          for {
            trans <- ZIO.environment[Transport[Address]].map(_.get)
            fiber <- trans
                      .bind(addr)
                      .take(1)
                      .runHead
                      .flatMap(c => ZStream.managed(c.get).flatMap(_.receive.take(1)).runHead)
                      .fork
            _      <- trans.send(addr, payload).retry(Schedule.spaced(10.milliseconds))
            result <- fiber.join
          } yield assert(result)(isSome(equalTo(payload)))
        }
      }
    ) @@ sequential).provideCustomLayer(environment)
}
