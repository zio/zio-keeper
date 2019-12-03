package zio.membership

import zio._
import zio.duration._
import zio.macros.delegate._
import zio.membership.transport.tcp
import zio.membership.hyparview.HyParView
import zio.nio.SocketAddress
import upickle.default._
import zio.nio.InetSocketAddress

object Main1 extends zio.App {

  override def run(args: List[String]) = {
    val env =
      ZManaged.environment[ZEnv] @@
        tcp.withTcpTransport(64, 100.seconds, 100.seconds)
    implicit val rw: ReadWriter[InetSocketAddress] =
      macroRW[(String, Int)]
        .bimap[InetSocketAddress](
          i => (i.hostString, i.port),
          { case (host: String, port: Int) => unsafeRun(SocketAddress.inetSocketAddress(host, port)) }
        )

    SocketAddress
      .inetSocketAddress(8010)
      .flatMap { addr =>
        (env >>> HyParView(
          addr,
          10,
          10,
          4,
          2,
          3,
          3,
          3,
          Schedule.spaced(200.millis),
          Schedule.spaced(200.millis)
        )).use { mem =>
          for {
            target <- SocketAddress.inetSocketAddress(8000)
            _      <- mem.membership.connect(target)
            out    <- ZIO.never.as(0)
          } yield out
        }
      }
      .catchAll(e => console.putStr(e.toString()).as(1))
  }

}
