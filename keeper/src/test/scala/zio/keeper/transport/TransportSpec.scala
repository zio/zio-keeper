//package zio.keeper.transport
//
//import zio._
//import zio.clock.Clock
//import zio.console.Console
//import zio.duration._
//import zio.console._
//import zio.keeper.TransportError.ExceptionWrapper
//import zio.logging.slf4j.Logging
//import zio.macros.delegate._
//import zio.nio.SocketAddress
//import zio.test.Assertion._
//import zio.test._
//import zio.test.environment.{Live, TestClock, TestConsole}
//
//object TransportSpec
//    extends DefaultRunnableSpec({
//      // todo: actually find free port
//      val freePort = ZIO.succeed(9010)
//
//      val withTransport = tcp.withTcpTransport(10.seconds, 10.seconds)
//
//      val environment =
//        for {
//          test <- (ZIO.environment[TestClock with TestConsole with Logging] @@ withTransport)
//          live <- (Live.live(ZIO.environment[Clock with Console]) @@ withTransport).flatMap(Live.make)
//          env  <- ZIO.succeed(live) @@ enrichWith(test)
//        } yield env
//
//      def bindAndWaitForValue(
//        addr: SocketAddress,
//        startPromise: Promise[Nothing, Unit],
//        handler: ChannelOut => UIO[Unit] = _ => ZIO.unit
//      ) =
//        for {
//          q <- Queue.bounded[Chunk[Byte]](10)
//          h = (out: ChannelOut) => {
//            for {
//              _    <- handler(out)
//              data <- out.read
//              _    <- q.offer(data)
//            } yield ()
//          }.catchAll(ex => putStrLn("error in server: " + ex).provide(Console.Live))
//          p <- bind(addr)(h).use_(startPromise.succeed(()) *> q.take)
//        } yield p
//
//      suite("TcpTransport")(
//        testM("can send and receive messages") {
//          checkM(Gen.listOf(Gen.anyByte)) {
//            bytes =>
//              val payload = Chunk.fromIterable(bytes)
//
//              environment >>> Live.live(for {
//                port         <- freePort
//                addr         <- SocketAddress.inetSocketAddress(port)
//                startPromise <- Promise.make[Nothing, Unit]
//                chunk        <- bindAndWaitForValue(addr, startPromise).fork
//                _            <- startPromise.await
//                _            <- connect(addr).use(_.send(payload).retry(Schedule.spaced(10.milliseconds)))
//                result       <- chunk.join
//              } yield assert(result, equalTo(payload)))
//          }
//        },
//        testM("we should be able to close the client connection") {
//          environment >>> Live.live(for {
//            port    <- freePort.map(_ + 2)
//            addr    <- SocketAddress.inetSocketAddress(port)
//            promise <- Promise.make[Nothing, Unit]
//            result <- bind(addr)(channel => channel.close.ignore.repeat(Schedule.doUntilM(_ => promise.isDone))).use_(
//                       connect(addr).use(client => client.read.ignore) *>
//                         connect(addr).use(client => client.read).either <*
//                         promise.succeed(())
//                     )
//          } yield (result match {
//            case Right(_) =>
//              assert(false, equalTo(true))
//            case Left(ex: ExceptionWrapper) =>
//              assert(ex.throwable.getMessage, equalTo("Connection reset by peer"))
//            case Left(_) =>
//              assert(false, equalTo(true))
//          }))
//
//        },
//        testM("handles interrupts like a champ") {
//          val payload = Chunk.single(Byte.MaxValue)
//
//          environment >>> Live.live(for {
//            latch  <- Promise.make[Nothing, Unit]
//            port   <- freePort.map(_ + 1)
//            addr   <- SocketAddress.inetSocketAddress(port)
//            fiber  <- bindAndWaitForValue(addr, latch, _ => ZIO.never).fork
//            _      <- latch.await
//            _      <- connect(addr).use(_.send(payload)).retry(Schedule.spaced(10.milliseconds))
//            result <- fiber.interrupt
//          } yield assert(result, isInterrupted))
//        }
//      )
//    })
