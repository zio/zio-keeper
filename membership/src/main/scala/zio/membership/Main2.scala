package zio.membership

import zio._
import zio.duration._
import zio.membership.transport.Address
import zio.membership.transport.tcp.Tcp
import zio.random.Random
import zio.logging.slf4j.Slf4jLogger
import zio.clock.Clock
import zio.membership.hyparview.{ HyParView, HyParViewConfig }
import upickle.default._
import zio.membership.hyparview.TRandom

object Main2 extends zio.App {

  implicit private val rw: ReadWriter[Address] =
    macroRW[(String, Int)]
      .bimap[Address](
        i => (i.host, i.port),
        { case (host, port) => Address(host, port) }
      )

  private val env = {
    val address   = Address("localhost", 8081)
    val tRandom   = Random.live >>> TRandom.live
    val logging   = Slf4jLogger.make((_, msg) => msg)
    val transport = (Clock.live ++ logging) >>> Tcp.live(64, 10.seconds, 10.seconds)
    val hpvc      = HyParViewConfig.staticConfig(10, 10, 4, 2, 3, 3, 3, 256, 256, 16)
    val hpv = (tRandom ++ logging ++ transport ++ hpvc ++ Clock.live) >>>
      HyParView.live(address, Schedule.spaced(2.seconds))
    (tRandom ++ logging ++ transport ++ hpvc ++ hpv)
  }

  override def run(args: List[String]) =
    env.build
      .use { environment =>
        for {
          mem <- ZIO.environment[Membership[Address]].provide(environment)
          _   <- mem.get.join(Address("localhost", 8080))
          out <- ZIO.never.as(0)
        } yield out
      }
      .catchAll(e => console.putStr(e.toString()).as(1))
}
