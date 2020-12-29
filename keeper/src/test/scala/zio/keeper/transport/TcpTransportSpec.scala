package zio.keeper.transport

import java.net.InetAddress

import javax.net.ServerSocketFactory
import zio.clock.Clock
import zio.keeper.{ ByteCodec, KeeperSpec, NodeAddress, TransportError }
import zio.logging.Logging
import zio.random.Random
import zio.test.Assertion.{ equalTo, isInterrupted, isSome, succeeds }
import zio.test.TestAspect.{ flaky, sequential, timeout }
import zio.test.environment.Live
import zio.test._
import zio._
import zio.Schedule
import zio.duration._
import zio.logging.log

object TcpTransportSpec extends KeeperSpec {

  val spec = (suite("TcpTransport")(
    testM("can send and receive messages") {
      checkNM(20)(Gen.listOf(Gen.anyByte)) { bytes =>
        val payload = Chunk.fromIterable(bytes)

        for {
          addr <- makeAddr
          chunk <- Transport
                    .bind(addr)
                    .mapM(_.use(_.receive.runHead))
                    .take(1)
                    .runHead
                    .map(_.flatten)
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
                      .map(_.map(_.unbatchOutputM(ByteCodec.decode[Chunk[Chunk[Byte]]])))
                      .mapM(_.use(_.receive.take(2).runCollect))
                      .take(1)
                      .runCollect
                      .map(_.flatten)
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
                  .mapM(_.use(_.receive.tap(_ => latch.succeed(())).runDrain))
                  .runDrain
                  .fork
        result <- Transport.connect(addr).use { con =>
                   con.send(payload) *> latch.await *> fiber.interrupt
                 }
      } yield assert(result)(isInterrupted)
    },
    testM("can send messages after bind stream is closed") {
      val payload = Chunk.single(Byte.MaxValue)

      for {
        addr  <- makeAddr
        latch <- Promise.make[Nothing, Unit]
        chunk <- Transport
                  .bind(addr)
                  .take(1)
                  .runHead
                  .mapError(Some(_))
                  .flatMap { con =>
                    latch.succeed(()) *>
                      con.fold[IO[Option[TransportError], Option[Chunk[Byte]]]](ZIO.fail(None))(
                        _.use(_.receive.runHead).mapError(Some(_))
                      )
                  }
                  .fork
        _ <- Transport
              .connect(addr)
              .use { con =>
                latch.await *> con.send(payload)
              }
              .ignore
        result <- chunk.await
        _      <- log.info(result.toString())
      } yield assert(result)(succeeds(isSome(equalTo(payload))))
    },
    testM("closes stream when the connected stream closes - client") {
      for {
        addr <- makeAddr
        fiber <- Transport
                  .bind(addr)
                  .take(1)
                  .mapM(_.use(_.receive.runDrain))
                  .runDrain
                  .fork
        _      <- Transport.connect(addr).useNow
        result <- fiber.await
      } yield assert(result)(succeeds(equalTo(())))
    },
    testM("closes stream when the connected stream closes - server") {
      for {
        addr <- makeAddr
        fiber <- Transport
                  .bind(addr)
                  .take(1)
                  .mapM(_.useNow)
                  .runDrain
                  .fork
        _      <- Transport.connect(addr).use(_.receive.runDrain)
        result <- fiber.await
      } yield assert(result)(succeeds(equalTo(())))
    },
    testM("respects max connections - receive") {
      val limit       = 10
      val connections = 15
      val payload     = Chunk.single(Byte.MaxValue)
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
        f1 <- Transport
               .bind(addr)
               .mapMPar(connections) {
                 _.use { con =>
                   con.send(payload) *> latch.await
                 }
               }
               .take(connections.toLong)
               .runDrain
               .fork
               .provideSomeLayer(tcp.make(limit, Schedule.spaced(10.millis)))
        f2     <- ZIO.collectAll(List.fill(connections)(connect.fork))
        _      <- ZIO.foreach(latches)(_.await)
        result <- ref.get
        _      <- latch.succeed(())
        _      <- f1.await
        _      <- ZIO.foreach(f2)(_.await)
      } yield assert(result)(equalTo(limit))
    },
    testM("respects max connections - send") {
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
              .mapMPar(waiters) {
                _.use(_.send(Chunk.empty) *> latch.await)
              }
              .runDrain
              .race(latch.await)
              .fork
        _      <- ZIO.collectAll_(List.fill(senders)(connect)).provideSomeLayer(tcp.make(limit, Schedule.spaced(10.millis)))
        _      <- ZIO.foreach(latches)(_.await)
        result <- ref.get
        _      <- latch.succeed(())
      } yield assert(result)(equalTo(limit))
    }
  ) @@ timeout(15.seconds) @@ sequential @@ flaky).provideCustomLayer(environment)

  private lazy val environment =
    ((Clock.live ++ Logging.ignore) >+> tcp.make(128, Schedule.spaced(10.millis)))

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
