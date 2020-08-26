package zio.keeper.transport

import java.net.InetAddress

import javax.net.ServerSocketFactory
import zio.clock.Clock
import zio.keeper.{ ByteCodec, KeeperSpec, NodeAddress }
import zio.logging.Logging
import zio.random.Random
import zio.stream.ZStream
import zio.test.Assertion.{ equalTo, isInterrupted, isSome }
import zio.test.TestAspect.{ flaky, sequential, timeout }
import zio.test.environment.Live
import zio.test._
import zio._
import zio.duration._

object TcpTransportSpec extends KeeperSpec {

  val spec = (suite("TcpTransport")(
    testM("can send and receive messages") {
      checkNM(20)(Gen.listOf(Gen.anyByte)) { bytes =>
        val payload = Chunk.fromIterable(bytes)

        for {
          addr <- makeAddr
          chunk <- Transport
                    .bind(addr)
                    .flatMap(c => c.receive.take(1))
                    .take(1)
                    .runHead
                    .fork
          _      <- Transport.send(addr, payload)
          result <- chunk.join
        } yield assert(result)(isSome(equalTo(payload)))
      }
    },
    testM("can send and receive batched messages") {
      checkNM(20)(Gen.chunkOf(Gen.anyByte), Gen.chunkOf(Gen.anyByte)) {
        case (payload1, payload2) =>
          for {
            addr <- makeAddr
            chunk <- Transport
                      .bind(addr)
                      .map(_.unbatchOutputM(ByteCodec.decode[Chunk[Chunk[Byte]]]))
                      .flatMap(c => c.receive.take(2))
                      .take(2)
                      .runCollect
                      .fork
            result <- Transport
                       .connect(addr)
                       .flatMap(_.batchInputM(ByteCodec.encode[Chunk[Chunk[Byte]]]))
                       .use { con =>
                         con.send(payload1) *> con.send(payload2) *> chunk.join
                       }
          } yield assert(result.toList)(equalTo(List(payload1, payload2)))
      }
    },
    testM("handles interrupts") {
      val payload = Chunk.single(Byte.MaxValue)

      for {
        latch <- Promise.make[Nothing, Unit]
        addr  <- makeAddr
        fiber <- Transport
                  .bind(addr)
                  .flatMap(c => c.receive.take(1).tap(_ => latch.succeed(())))
                  .take(2)
                  .runDrain
                  .fork
        _      <- Transport.send(addr, payload)
        result <- latch.await *> fiber.interrupt
      } yield assert(result)(isInterrupted)
    },
    testM("respects max connections") {
      for {
        ref    <- Ref.make(0)
        latch  <- Promise.make[Nothing, Unit]
        latch0 <- Promise.make[Nothing, Unit]
        addr   <- makeAddr
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
                   con.receive.take(1)
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
  ) @@ timeout(15.seconds) @@ sequential @@ flaky).provideCustomLayer(environment)

  private lazy val environment =
    ((Clock.live ++ Logging.ignore) >+> tcp.make(10, 10.seconds, 10.seconds))

  private def findAvailableTCPPort(minPort: Int, maxPort: Int): URIO[Live, Int] = {
    val portRange = maxPort - minPort
    def isPortAvalable(port: Int): UIO[Boolean] =
      ZIO
        .effect {
          val serverSocket =
            ServerSocketFactory.getDefault.createServerSocket(port, 1, InetAddress.getByName("localhost"))
          serverSocket.close()
        }
        .fold(_ => false, _ => true)

    val nextRandomPort: URIO[Random, Int] =
      random.nextIntBounded(portRange + 1).map(_ + minPort)

    def go(counter: Int = 0): URIO[Random, Int] =
      if (counter > portRange) ZIO.dieMessage("No port found in range")
      else
        nextRandomPort.flatMap(port => ZIO.ifM(isPortAvalable(port))(ZIO.succeed(port), go(counter + 1)))
    if ((minPort <= 0) || (maxPort < minPort) || (maxPort > 65535)) ZIO.dieMessage("Invalid port range")
    else Live.live(go())
  }

  private val makeAddr = findAvailableTCPPort(49152, 65535).flatMap(NodeAddress.local)

}
