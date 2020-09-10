package zio.keeper.transport

import java.net.InetAddress

import javax.net.ServerSocketFactory
import zio.clock.Clock
import zio.keeper.{ ByteCodec, KeeperSpec, NodeAddress }
import zio.logging.Logging
import zio.random.Random
import zio.test.Assertion.{ equalTo, isInterrupted, isSome, succeeds }
import zio.test.TestAspect.{ sequential, timeout }
import zio.test.environment.Live
import zio.test._
import zio._
import zio.Schedule
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
                  .flatMap(c => c.receive.tap(_ => latch.succeed(())))
                  .runDrain
                  .fork
        result <- Transport.connect(addr).use { con =>
                   con.send(payload) *> latch.await *> fiber.interrupt
                 }
      } yield assert(result)(isInterrupted)
    },
    testM("closes stream when the connected stream closes - client") {
      for {
        addr <- makeAddr
        fiber <- Transport
                  .bind(addr)
                  .take(1)
                  .flatMap(_.receive)
                  .runDrain
                  .fork
        _      <- Transport.connect(addr).use_(ZIO.unit)
        result <- fiber.await
      } yield assert(result)(succeeds(equalTo(())))
    },
    testM("closes stream when the connected stream closes - server") {
      for {
        latch <- Promise.make[Nothing, Unit]
        addr  <- makeAddr
        f1    <- (latch.await *> Transport.connect(addr).use(_.receive.runDrain)).fork
        f2 <- Transport
               .bind(addr)
               .take(1)
               .runDrain
               .fork
        _      <- latch.succeed(())
        result <- f1.await <* f2.await
      } yield assert(result)(succeeds(equalTo(())))
    },
    testM("respects max connections") {
      val limit   = 10
      val senders = 15
      val waiters = 20
      for {
        ref     <- Ref.make(0)
        latch   <- Promise.make[Nothing, Unit]
        latches <- ZIO.collectAll(List.fill(limit)(Promise.make[Nothing, Unit]))
        completeOne = ZIO.foldLeft(latches)(true) {
          case (true, p) =>
            p.succeed(()).map(!_)
          case (false, _) =>
            ZIO.succeedNow(false)
        }
        addr <- makeAddr
        connect = Transport
          .connect(addr)
          .use { con =>
            con.receive.runHead *>
              ref.update(_ + 1) *>
              completeOne *>
              latch.await
          }
          .fork

        _ <- Transport
              .bind(addr)
              .mapMPar(waiters) { con =>
                con.send(Chunk.empty) *> latch.await
              }
              .runDrain
              .race(latch.await)
              .fork
        _      <- ZIO.collectAll_(List.fill(senders)(connect))
        _      <- ZIO.foreach(latches)(_.await)
        result <- ref.get
        _      <- latch.succeed(())
      } yield assert(result)(equalTo(limit))
    }
  ) @@ timeout(15.seconds) @@ sequential).provideCustomLayer(environment)

  private lazy val environment =
    ((Clock.live ++ Logging.ignore) >+> tcp.make(10, Schedule.spaced(10.millis)))

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
