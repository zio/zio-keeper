package zio.keeper.transport.testing

import zio.keeper.KeeperSpec
import zio.keeper.transport.Transport
import zio.test.Assertion._
import zio.test.TestAspect.nonFlaky
import zio.test._
import zio._
import zio.keeper.transport.testing.TestTransport.{ asNode, awaitAvailable, setConnectivity }
import zio.keeper.TransportError

object TestTransportSpec extends KeeperSpec {

  val spec = {

    val environment = TestTransport.make

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
              _ <- asNode[TestTransport, TransportError, Unit](address(1)) {
                    awaitAvailable(address(0)) *> Transport.send(address(0), payload)
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
          _ <- asNode[TestTransport, TransportError, Unit](address(1)) {
                awaitAvailable(address(0)) *> Transport.send(address(0), payload)
              }
          result <- latch.await *> fiber.interrupt
        } yield result
        assertM(io.run)(succeeds(isInterrupted)).provideCustomLayer(environment)
      },
      testM("can not receive messages after bind stream is closed") {
        checkM(Gen.listOf(Gen.anyByte)) {
          bytes =>
            val payload = Chunk.fromIterable(bytes)

            val io =
              for {
                latch <- Promise.make[Nothing, Unit]
                fiber <- asNode[TestTransport, Option[TransportError], Option[Chunk[Byte]]](address(0)) {
                          Transport
                            .bind(address(0))
                            .take(1)
                            .runHead
                            .mapError(Some(_))
                            .flatMap { con =>
                              latch.succeed(()) *>
                                con.fold[IO[Option[TransportError], Option[Chunk[Byte]]]](ZIO.fail(None))(
                                  _.receive.runHead.mapError(Some(_))
                                )
                            }
                        }.fork
                _ <- asNode[TestTransport, TransportError, Unit](address(1)) {
                      awaitAvailable(address(0)) *> Transport.connect(address(0)).use { con =>
                        latch.await *> con.send(payload)
                      }
                    }
                result <- fiber.join
              } yield result
            assertM(io.run)(fails(anything)).provideCustomLayer(environment)
        }
      },
      testM("can not send messages after bind stream is closed") {
        checkM(Gen.listOf(Gen.anyByte)) {
          bytes =>
            val payload = Chunk.fromIterable(bytes)

            val io =
              for {
                latch <- Promise.make[Nothing, Unit]
                fiber <- asNode[TestTransport, Option[TransportError], Option[Chunk[Byte]]](address(0)) {
                          Transport
                            .bind(address(0))
                            .take(1)
                            .runHead
                            .mapError(Some(_))
                            .flatMap { con =>
                              latch.succeed(()) *>
                                con.fold[IO[Option[TransportError], Option[Chunk[Byte]]]](ZIO.fail(None))(
                                  _.receive.runHead.mapError(Some(_))
                                )
                            }
                        }.fork
                result <- asNode[TestTransport, TransportError, Unit](address(1)) {
                           awaitAvailable(address(0)) *> Transport.connect(address(0)).use { con =>
                             latch.await *> con.send(payload)
                           }
                         }.run
                _ <- fiber.await
              } yield result
            assertM(io)(fails(anything)).provideCustomLayer(environment)
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
          result <- asNode[TestTransport, TransportError, Option[Chunk[Byte]]](address(1)) {
                     awaitAvailable(address(0)) *> Transport.connect(address(0)).use_ {
                       setConnectivity((_, _) => false) *> fiber.join
                     }
                   }
        } yield result
        assertM(io.run)(fails(anything)).provideCustomLayer(environment)
      },
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
                  result <- asNode[TestTransport, TransportError, Unit](address(1)) {
                             awaitAvailable(address(0)) *> Transport.connect(address(0)).use { channel =>
                               setConnectivity((_, _) => false) *> channel
                                 .send(Chunk.fromIterable(bytes)) <* fiber.await
                             }
                           }
                } yield result
              assertM(io.run)(fails(anything)).provideCustomLayer(environment)
          }
        }
      }
    )
  } @@ nonFlaky(20)
}
