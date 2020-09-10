package zio.keeper.transport.testing

import zio.clock.Clock
import zio.keeper.KeeperSpec
import zio.keeper.transport.Transport
import zio.test.Assertion._
import zio.test.TestAspect.nonFlaky
import zio.test._
import zio.{ Chunk, Promise, Schedule, ZLayer }
import zio.duration._
import zio.keeper.transport.testing.TestTransport.asNode
import zio.keeper.TransportError

object InMemoryTransportSpec extends KeeperSpec {

  val spec = {

    val environment = Clock.live >>> (ZLayer.identity[Clock] ++ TestTransport.make())

    suite("InMemoryTransport")(
      testM("can send and receive messages") {
        checkM(Gen.listOf(Gen.anyByte)) {
          bytes =>
            val payload = Chunk.fromIterable(bytes)
            val io = for {
              chunk <- asNode[TestTransport, TransportError, Option[Chunk[Byte]]](address(0)) {
                        Transport
                          .bind(address(0))
                          .flatMap(_.receive.take(1))
                          .take(1)
                          .runHead
                      }.fork
              _ <- asNode[TestTransport with Clock, TransportError, Unit](address(1)) {
                    Transport.send(address(0), payload).retry(Schedule.spaced(10.milliseconds))
                  }
              result <- chunk.join
            } yield result
            assertM(io.run)(succeeds(isSome(equalTo(payload)))).provideCustomLayer(environment)
        }
      },
      testM("handles interrupts") {
        val payload = Chunk.single(Byte.MaxValue)

        val io = for {
          latch <- Promise.make[Nothing, Unit]
          fiber <- asNode[TestTransport, TransportError, Unit](address(0)) {
                    Transport
                      .bind(address(0))
                      .flatMap(_.receive.take(1).tap(_ => latch.succeed(())))
                      .take(2)
                      .runDrain
                  }.fork
          _ <- asNode[TestTransport with Clock, TransportError, Unit](address(1)) {
                Transport.send(address(0), payload).retry(Schedule.spaced(10.milliseconds))
              }
          result <- latch.await *> fiber.interrupt
        } yield result
        assertM(io.run)(succeeds(isInterrupted)).provideCustomLayer(environment)
      },
      testM("can receive messages after bind stream is closed") {
        checkM(Gen.listOf(Gen.anyByte)) {
          bytes =>
            val payload = Chunk.fromIterable(bytes)

            val io = for {
              fiber <- asNode[TestTransport, TransportError, Option[Chunk[Byte]]](address(0)) {
                        Transport
                          .bind(address(0))
                          .flatMap(_.receive.take(1))
                          .take(1)
                          .runHead
                      }.fork
              _ <- asNode[TestTransport with Clock, TransportError, Unit](address(1)) {
                    Transport.send(address(0), payload).retry(Schedule.spaced(10.milliseconds))
                  }
              result <- fiber.join
            } yield result
            assertM(io.run)(succeeds(isSome(equalTo(payload)))).provideCustomLayer(environment)
        }
      },
      testM("Fails receive if connectivity is interrupted") {
        val io = for {
          fiber <- asNode[TestTransport, TransportError, Option[Chunk[Byte]]](address(0)) {
                    Transport
                      .bind(address(0))
                      .flatMap(_.receive.take(1))
                      .take(1)
                      .runHead
                  }.fork
          result <- asNode[TestTransport with Clock, TransportError, Option[Chunk[Byte]]](address(1)) {
                     Transport.connect(address(0)).retry(Schedule.spaced(10.milliseconds)).use_ {
                       TestTransport.setConnectivity((_, _) => false) *> fiber.join
                     }
                   }
        } yield result
        assertM(io.run)(fails(anything)).provideCustomLayer(environment)
      } @@ nonFlaky(20),
      testM("Fails send if connectivity is interrupted") {
        {
          checkM(Gen.listOf(Gen.anyByte)) {
            bytes =>
              val io =
                for {
                  fiber <- asNode[TestTransport, TransportError, Option[Chunk[Byte]]](address(0)) {
                            Transport
                              .bind(address(0))
                              .flatMap(_.receive.take(1))
                              .take(1)
                              .runHead
                          }.fork
                  result <- asNode[TestTransport with Clock, TransportError, Unit](address(1)) {
                             Transport.connect(address(0)).retry(Schedule.spaced(10.milliseconds)).use { channel =>
                               TestTransport.setConnectivity((_, _) => false) *> channel
                                 .send(Chunk.fromIterable(bytes)) <* fiber.await
                             }
                           }
                } yield result
              assertM(io.run)(fails(anything)).provideCustomLayer(environment)
          }
        }
      }
    )
  }
}
