package zio.keeper.transport

import zio._
import zio.duration._
import zio.nio.SocketAddress
import zio.test._
import zio.test.Assertion._
import zio.test.environment.Live
import TransportUtil._

object UdpSpec extends DefaultRunnableSpec({

  val freePort = UIO.succeed(9011)

  val environment =
    transportEnvironment(udp.udpTransport(10.seconds, 10.seconds, 256))

  suite("UdpTransport")(
    testM("can send and receive messages") {
      checkM(Gen.listOf(Gen.anyByte)) { bytes =>
        val payload = Chunk.fromIterable(bytes)

        environment >>> Live.live(for {
          port         <- freePort
          addr         <- SocketAddress.inetSocketAddress(port)
          startPromise <- Promise.make[Nothing, Unit]
          chunk        <- bindAndWaitForValue(addr, startPromise).fork
          _            <- UIO.effectTotal(println("blah"))
          _            <- startPromise.await
          _            <- UIO.effectTotal(println("blah again"))
          _            <- connect(addr).use(_.send(payload).retry(Schedule.spaced(10.milliseconds)))
          _            <- UIO.effectTotal(println("blah yet again"))
          result       <- chunk.join
        } yield assert(result, equalTo(payload)))
      }
    }
  )
})
