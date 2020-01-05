package zio.membership

import zio._
import zio.duration._
import zio.macros.delegate._
import zio.macros.delegate.syntax._
import zio.membership.transport.tcp
import zio.membership.hyparview._
import upickle.default._
import zio.membership.transport.Address
import zio.logging.Logging
import zio.membership.hyparview.TRandom

object Main1 extends zio.App {

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
        Address("localhost", 8080),
        Schedule.spaced(2.seconds)
      )
  }

  override def run(args: List[String]) =
    // Fiber.dump.flatMap(ZIO.foreach(_)(_.prettyPrintM.flatMap(console.putStrLn))).delay(10.seconds).uninterruptible.fork *>
    env
      .use(_ => ZIO.never.unit)
      .foldM(
        e => console.putStrLn(e.toString()).as(1),
        _ => ZIO.succeed(0)
      )

}
