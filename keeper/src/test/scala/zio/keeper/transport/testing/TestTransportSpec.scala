package zio.keeper.transport.testing

import zio.keeper.KeeperSpec
import zio.keeper.transport.Transport
import zio.test.Assertion._
import zio.test.TestAspect.nonFlaky
import zio.test._
import zio._
import zio.stream.Stream
import zio.keeper.transport.testing.TestTransport.{ asNode, awaitAvailable, setConnectivity }
import zio.keeper.TransportError
import zio.keeper.NodeAddress

object TestTransportSpec extends KeeperSpec {

  val spec = {

    val environment = TestTransport.make

    val addr1Ip = Chunk[Byte](0, 0, 0, 1)
    val addr1   = NodeAddress(addr1Ip, 0)
    val addr2Ip = Chunk[Byte](0, 0, 0, 2)

    suite("InMemoryTransport")(
      testM("can send and receive messages") {
        checkM(Gen.listOf(Gen.anyByte)) {
          bytes =>
            val payload = Chunk.fromIterable(bytes)
            val io = for {
              chunk <- asNode[TestTransport, TransportError, Option[Chunk[Byte]]](addr1Ip) {
                        Transport
                          .bind(addr1)
                          .flatMap(Stream.managed(_))
                          .flatMap(_.receive.take(1))
                          .take(1)
                          .runHead
                      }.fork
              _ <- asNode[TestTransport, TransportError, Unit](addr2Ip) {
                    awaitAvailable(addr1) *> Transport.send(addr1, payload)
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
          fiber <- asNode[TestTransport, TransportError, Unit](addr1Ip) {
                    Transport
                      .bind(addr1)
                      .flatMap(Stream.managed(_))
                      .flatMap(_.receive.take(1).tap(_ => latch.succeed(())))
                      .take(2)
                      .runDrain
                  }.fork
          _ <- asNode[TestTransport, TransportError, Unit](addr2Ip) {
                awaitAvailable(addr1) *> Transport.send(addr1, payload)
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
                fiber <- asNode[TestTransport, Option[TransportError], Option[Chunk[Byte]]](addr1Ip) {
                          Transport
                            .bind(addr1)
                            .take(1)
                            .runHead
                            .mapError(Some(_))
                            .flatMap { con =>
                              latch.succeed(()) *>
                                con.fold[IO[Option[TransportError], Option[Chunk[Byte]]]](ZIO.fail(None))(
                                  _.use(_.receive.runHead).mapError(Some(_))
                                )
                            }
                        }.fork
                _ <- asNode[TestTransport, TransportError, Unit](addr2Ip) {
                      awaitAvailable(addr1) *> Transport.connect(addr1).use { con =>
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
                fiber <- asNode[TestTransport, Option[TransportError], Option[Chunk[Byte]]](addr1Ip) {
                          Transport
                            .bind(addr1)
                            .take(1)
                            .runHead
                            .mapError(Some(_))
                            .flatMap { con =>
                              latch.succeed(()) *>
                                con.fold[IO[Option[TransportError], Option[Chunk[Byte]]]](ZIO.fail(None))(
                                  _.use(_.receive.runHead).mapError(Some(_))
                                )
                            }
                        }.fork
                result <- asNode[TestTransport, TransportError, Unit](addr2Ip) {
                           awaitAvailable(addr1) *> Transport.connect(addr1).use { con =>
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
          fiber <- asNode[TestTransport, TransportError, Option[Chunk[Byte]]](addr1Ip) {
                    Transport
                      .bind(addr1)
                      .flatMap(Stream.managed(_))
                      .flatMap(_.receive.take(1))
                      .take(1)
                      .runHead
                  }.fork
          result <- asNode[TestTransport, TransportError, Option[Chunk[Byte]]](addr2Ip) {
                     awaitAvailable(addr1) *> Transport.connect(addr1).use_ {
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
                  fiber <- asNode[TestTransport, TransportError, Option[Chunk[Byte]]](addr1Ip) {
                            Transport
                              .bind(addr1)
                              .flatMap(Stream.managed(_))
                              .flatMap(_.receive.take(1))
                              .take(1)
                              .runHead
                          }.fork
                  result <- asNode[TestTransport, TransportError, Unit](addr2Ip) {
                             awaitAvailable(addr1) *> Transport.connect(addr1).use { channel =>
                               setConnectivity((_, _) => false) *> channel
                                 .send(Chunk.fromIterable(bytes)) <* fiber.await
                             }
                           }
                } yield result
              assertM(io.run)(fails(anything)).provideCustomLayer(environment)
          }
        }
      },
      testM("Cannot bind twice to the same address") {
        val f1 = asNode[TestTransport, TransportError, Unit](addr1Ip) {
          Transport
            .bind(addr1)
            .flatMap(Stream.managed(_))
            .flatMap(_.receive)
            .take(1)
            .runDrain
        }
        val f2 = asNode[TestTransport, TransportError, Unit](addr1Ip) {
          Transport
            .bind(addr1)
            .flatMap(Stream.managed(_))
            .flatMap(_.receive)
            .take(1)
            .runDrain
        }
        assertM((f1 <&> f2).run)(fails(anything)).provideCustomLayer(environment)
      },
      testM("Cannot bind to address belonging to different ip") {
        val io = asNode[TestTransport, TransportError, Unit](addr2Ip) {
          Transport
            .bind(addr1)
            .flatMap(Stream.managed(_))
            .flatMap(_.receive)
            .take(1)
            .runDrain
        }
        assertM(io.run)(fails(anything)).provideCustomLayer(environment)
      }
    )
  } @@ nonFlaky(20)
}
