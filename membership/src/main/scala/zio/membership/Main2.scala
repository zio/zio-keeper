package zio.membership

import zio._
import zio.duration._
import zio.macros.delegate._
import zio.macros.delegate.syntax._
import zio.membership.transport.{ Address, tcp }
import zio.membership.hyparview.HyParView
import upickle.default._
import zio.logging.Logging

object Main2 extends zio.App {

  override def run(args: List[String]) = {
    val env =
      ZManaged.environment[ZEnv] @@
        enrichWith[Logging[String]](new zio.logging.slf4j.Slf4jLogger.Live {
          def formatMessage(msg: String) = ZIO.succeed(msg)
        }) @@
        tcp.withTcpTransport(64, 100.seconds, 100.seconds)
    implicit val rw: ReadWriter[Address] =
      macroRW[(String, Int)]
        .bimap[Address](
          i => (i.host, i.port),
          { case (host: String, port: Int) => Address(host, port) }
        )

    (env >>> HyParView(
      Address("localhost", 8020),
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
    )).use { mem =>
        for {
          _   <- mem.membership.join(Address("localhost", 8000))
          out <- ZIO.never.as(0)
        } yield out
      }
      .catchAll(e => console.putStr(e.toString()).as(1))
  }
}
