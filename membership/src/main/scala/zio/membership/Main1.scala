package zio.membership

import zio._
import zio.duration._
import zio.membership.transport.tcp
import zio.membership.hyparview._
import upickle.default._
import zio.membership.transport.Address
import zio.membership.hyparview.TRandom
import zio.random.Random
import zio.clock.Clock
import zio.logging.slf4j.Slf4jLogger

object Main1 extends zio.App {

  implicit private val rw: ReadWriter[Address] =
    macroRW[(String, Int)]
      .bimap[Address](
        i => (i.host, i.port),
        { case (host, port) => Address(host, port) }
      )

  private val env = {
    val address   = Address("localhost", 8080)
    val tRandom   = Random.live >>> TRandom.live
    val logging   = Slf4jLogger.make((_, msg) => msg)
    val transport = (Clock.live ++ logging) >>> tcp.live(64, 10.seconds, 10.seconds)
    val hpvc      = HyParViewConfig.staticConfig(10, 10, 4, 2, 3, 3, 3, 256, 256, 16)
    val hpv = (tRandom ++ logging ++ transport ++ hpvc ++ Clock.live) >>>
      HyParView.live(address, Schedule.spaced(2.seconds))
    (tRandom ++ logging ++ transport ++ hpvc ++ hpv)
  }

  override def run(args: List[String]) =
    // Fiber.dump.flatMap(ZIO.foreach(_)(_.prettyPrintM.flatMap(console.putStrLn))).delay(10.seconds).uninterruptible.fork *>
    env.build
      .use(_ => ZIO.never.unit)
      .foldM(
        e => console.putStrLn(e.toString()).as(1),
        _ => ZIO.succeed(0)
      )
}
