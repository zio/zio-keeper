package zio.keeper

import zio.ZIO
import zio.clock.Clock
import zio.console.Console
import zio.keeper.transport.tcp
import zio.macros.delegate.enrichWith
import zio.test.DefaultRunnableSpec
import zio.test.environment.{Live, TestClock, TestConsole}
import zio.duration._
import zio.test._
import zio.test.Assertion._
import zio.macros.delegate._

object ClusterSpec extends DefaultRunnableSpec({

  val freePort = ZIO.succeed(8010)

  val withTransport = tcp.withTcpTransport(10.seconds, 10.seconds)

  val environment =
    for {
      test <- (ZIO.environment[TestClock with TestConsole] @@ withTransport)
      live <- (Live.live(ZIO.environment[Clock with Console]) @@ withTransport).flatMap(Live.make)
      env  <- ZIO.succeed(live) @@ enrichWith(test)
    } yield env



  testM("blah"){
    assertM(ZIO.unit, equalTo("a"))
  }
})
