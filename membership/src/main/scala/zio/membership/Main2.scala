package zio.membership

import zio._
import zio.duration._
import zio.macros.delegate._
import zio.macros.delegate.syntax._
import zio.membership.transport.{ Address, tcp }
import zio.membership.hyparview.{ HyParView, HyParViewConfig }
import upickle.default._
import zio.logging.Logging
import zio.membership.hyparview.TRandom

object Main2 extends zio.App {

  val env = {

    implicit val rw: ReadWriter[Address] =
      macroRW[(String, Int)]
        .bimap[Address](
          i => (i.host, i.port),
          { case (host: String, port: Int) => Address(host, port) }
        )

    ZManaged.environment[ZEnv] @@
      TRandom.withTRandom @@
      enrichWith[Logging[String]](new zio.logging.slf4j.Slf4jLogger.Live {
        def formatMessage(msg: String) = ZIO.succeed(msg)
      }) @@
      tcp.withTcpTransport(
        64,
        10.seconds,
        10.seconds
      ) @@
      HyParViewConfig.withStaticConfig(
        10, 10, 4, 2, 3, 3, 3, 256, 256
      ) @@
      HyParView.withHyParView(
        Address("localhost", 8081),
        Schedule.spaced(2.seconds)
      )
  }

  override def run(args: List[String]) =
    env
      .use { mem =>
        for {
          _   <- mem.membership.join(Address("localhost", 8080))
          out <- ZIO.never.as(0)
        } yield out
      }
      .catchAll(e => console.putStr(e.toString()).as(1))
}
