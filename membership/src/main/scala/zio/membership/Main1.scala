package zio.membership

import zio._
import zio.duration._
import zio.macros.delegate.syntax._
import zio.membership.transport.tcp
import zio.membership.hyparview.HyParView
import upickle.default._
import zio.membership.transport.Address

object Main1 extends zio.App {

  override def run(args: List[String]) = {
    val env =
      ZManaged.environment[ZEnv] @@
        tcp.withTcpTransport(64, 100.seconds, 100.seconds)
    implicit val rw: ReadWriter[Address] =
      macroRW[(String, Int)]
        .bimap[Address](
          i => (i.host, i.port),
          { case (host: String, port: Int) => Address(host, port) }
        )

        (env >>> HyParView(
          Address("localhost", 8000),
          10,
          10,
          4,
          2,
          3,
          3,
          3,
          Schedule.spaced(2.seconds),
          Schedule.spaced(2.seconds),
          256,
          256,
          16
        )).useForever.catchAll(e => console.putStr(e.toString()).as(1))
  }

}
