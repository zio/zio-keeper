package zio.membership.transport

import zio._
import zio.clock.Clock
import zio.duration._
import zio.macros.delegate._
import zio.macros.delegate.syntax._
import zio.nio.SocketAddress
import zio.test.Assertion._
import zio.test._
import zio.test.environment.{ Live, TestClock }

object TransportSpec
    extends DefaultRunnableSpec({
      // todo: actually find free port
      val freePort = ZIO.succeed(8010)

      val withTransport = tcp.withTcpTransport(10, 10.seconds, 10.seconds)

      val environment =
        for {
          test <- (ZIO.environment[TestClock] @@ withTransport)
          live <- (Live.live(ZIO.environment[Clock]) @@ withTransport).flatMap(Live.make(_).toManaged_)
          env  <- ZManaged.succeed(live) @@ enrichWith(test)
        } yield env

      suite("TcpTransport")(
        testM("can send and receive messages") {
          checkM(Gen.listOf(Gen.anyByte)) {
            bytes =>
              val payload = Chunk.fromIterable(bytes)

              environment.use { env =>
                env.live.provide {
                  for {
                    trans  <- ZIO.environment[Transport[SocketAddress]].map(_.transport)
                    port   <- freePort
                    addr   <- SocketAddress.inetSocketAddress(port)
                    chunk  <- trans.bind(addr).take(1).flatMap(_.receive.take(1)).runCollect.map(_.head).fork
                    _      <- trans.send(addr, payload).retry(Schedule.spaced(10.milliseconds))
                    result <- chunk.join
                  } yield assert(result, equalTo(payload))
                }
              }
          }
        },
        testM("handles interrupts like a good boy") {
          val payload = Chunk.single(Byte.MaxValue)

          environment.use {
            env =>
              env.live.provide {
                for {
                  trans  <- ZIO.environment[Transport[SocketAddress]].map(_.transport)
                  latch  <- Promise.make[Nothing, Unit]
                  port   <- freePort.map(_ + 1)
                  addr   <- SocketAddress.inetSocketAddress(port)
                  fiber  <- trans.bind(addr).take(1).flatMap(_.receive.take(2).tap(_ => latch.succeed(()))).runDrain.fork
                  _      <- trans.send(addr, payload).retry(Schedule.spaced(10.milliseconds))
                  result <- latch.await *> fiber.interrupt
                } yield assert(result, isInterrupted)
              }
          }
        },
        testM("can receive messages after bind stream is closed") {
          checkM(Gen.listOf(Gen.anyByte)) { bytes =>
            val payload = Chunk.fromIterable(bytes)

            environment.use {
              env =>
                env.live.provide {
                  for {
                    trans <- ZIO.environment[Transport[SocketAddress]].map(_.transport)
                    port  <- freePort.map(_ + 2)
                    addr  <- SocketAddress.inetSocketAddress(port)
                    fiber <- trans
                              .bind(addr)
                              .take(1)
                              .runHead
                              .map(_.get)
                              .flatMap(_.receive.take(1).runCollect.map(_.head))
                              .fork
                    _      <- trans.send(addr, payload).retry(Schedule.spaced(10.milliseconds))
                    result <- fiber.join
                  } yield assert(result, equalTo(payload))
                }
            }
          }
        }
      )
    })
