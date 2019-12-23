package zio.membership.transport

import zio._
import zio.clock.Clock
import zio.duration._
import zio.macros.delegate._
import zio.macros.delegate.syntax._
import zio.test.environment.TestClock
import zio.test.environment.Live
import zio.stream.ZStream
import zio.test.TestAspect._

object TransportSpec
    extends DefaultRunnableSpec({
      val addr          = Address("localhost", 8081)
      val withTransport = tcp.withTcpTransport(10, 10.seconds, 10.seconds)

      val environment =
        for {
          test <- (ZIO.environment[TestClock] @@ withTransport)
          live <- (Live.live(ZIO.environment[Clock]) @@ withTransport).flatMap(Live.make(_))
          env = Mix[
            Clock with Transport[Address],
            Live[Clock with Transport[Address]]
          ].mix(test, live)
        } yield env

      suite("TcpTransport")(
        testM("can send and receive messages") {
          checkM(Gen.listOf(Gen.anyByte)) {
            bytes =>
              val payload = Chunk.fromIterable(bytes)

              environment.flatMap { env =>
                env.live.provide {
                  for {
                    trans <- ZIO.environment[Transport[Address]].map(_.transport)
                    chunk <- trans
                              .bind(addr)
                              .flatMap(c => ZStream.managed(c).flatMap(_.receive.take(1)))
                              .take(1)
                              .runHead
                              .fork
                    _      <- trans.send(addr, payload).retry(Schedule.spaced(10.milliseconds))
                    result <- chunk.join
                  } yield assert(result, isSome(equalTo(payload)))
                }
              }
          }
        },
        testM("handles interrupts") {
          val payload = Chunk.single(Byte.MaxValue)

          environment.flatMap {
            env =>
              env.live.provide {
                for {
                  trans <- ZIO.environment[Transport[Address]].map(_.transport)
                  latch <- Promise.make[Nothing, Unit]
                  fiber <- trans
                            .bind(addr)
                            .flatMap(c => ZStream.managed(c).flatMap(_.receive.take(1).tap(_ => latch.succeed(()))))
                            .take(2)
                            .runDrain
                            .fork
                  _      <- trans.send(addr, payload).retry(Schedule.spaced(10.milliseconds))
                  result <- latch.await *> fiber.interrupt
                } yield assert(result, isInterrupted)
              }
          }
        },
        testM("can receive messages after bind stream is closed") {
          checkM(Gen.listOf(Gen.anyByte)) {
            bytes =>
              val payload = Chunk.fromIterable(bytes)

              environment.flatMap { env =>
                env.live.provide {
                  for {
                    trans <- ZIO.environment[Transport[Address]].map(_.transport)
                    fiber <- trans
                              .bind(addr)
                              .flatMap(c => ZStream.managed(c).flatMap(_.receive.take(1)))
                              .take(1)
                              .runHead
                              .fork
                    _      <- trans.send(addr, payload).retry(Schedule.spaced(10.milliseconds))
                    result <- fiber.join
                  } yield assert(result, isSome(equalTo(payload)))
                }
              }
          }
        }
      ) @@ sequential
    })
