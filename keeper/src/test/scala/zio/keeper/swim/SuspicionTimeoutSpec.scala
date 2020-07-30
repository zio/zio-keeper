package zio.keeper.swim

import java.util.concurrent.TimeUnit

import zio.clock.Clock
import zio.duration._
import zio.keeper.{ KeeperSpec, NodeAddress }
import zio.logging.Logging
import zio.test.Assertion._
import zio.test._
import zio.test.environment.TestClock
import zio.{ Promise, ZIO, ZLayer }
import zio.clock._
import zio.keeper.SwimError.SuspicionTimeoutCancelled

object SuspicionTimeoutSpec extends KeeperSpec {

  val logger = Logging.console((_, line) => line)

  def testLayer(
    protocolInterval: Duration,
    suspicionAlpha: Int,
    suspicionBeta: Int,
    suspicionRequiredConfirmations: Int
  ) =
    (ZLayer.requires[Clock] ++ logger) >+> Nodes.live >+> SuspicionTimeout.live(
      protocolInterval,
      suspicionAlpha,
      suspicionBeta,
      suspicionRequiredConfirmations
    )

  val spec = suite("Suspicion timeout")(
    testM("schedule timeout with 100 nodes cluster") {
      for {
        promise <- Promise.make[Nothing, Long]
        _       <- ZIO.foreach(1 to 100)(i => Nodes.addNode(NodeAddress(Array(1, 2, 3, 4), i)))
        node    = NodeAddress(Array(1, 1, 1, 1), 1111)
        start   <- currentTime(TimeUnit.MILLISECONDS)
        _ <- SuspicionTimeout
              .registerTimeout(node)(
                currentTime(TimeUnit.MILLISECONDS).flatMap(current => promise.succeed(current - start))
              )
              .fork
        _           <- TestClock.adjust(50000.milliseconds)
        elapsedTime <- promise.await
      } yield assert(elapsedTime)(equalTo(4000L))
    }.provideCustomLayer(testLayer(1.second, 1, 2, 3)),
    testM("timeout should be decreased when another confirmation arrives") {
      for {
        promise <- Promise.make[Nothing, Long]
        _       <- ZIO.foreach(1 to 100)(i => Nodes.addNode(NodeAddress(Array(1, 2, 3, 4), i)))
        node    = NodeAddress(Array(1, 1, 1, 1), 1111)
        other   = NodeAddress(Array(2, 1, 1, 1), 1111)
        start   <- currentTime(TimeUnit.MILLISECONDS)
        _ <- SuspicionTimeout
              .registerTimeout(node)(
                currentTime(TimeUnit.MILLISECONDS).flatMap(current => promise.succeed(current - start))
              )
              .fork
        _           <- TestClock.adjust(150.milliseconds)
        _           <- SuspicionTimeout.incomingSuspect(node, other)
        _           <- TestClock.adjust(50000.milliseconds)
        elapsedTime <- promise.await
      } yield assert(elapsedTime)(equalTo(3000L))
    }.provideCustomLayer(testLayer(1.second, 1, 2, 3)),
    testM("should be able to cancel") {
      for {
        promise <- Promise.make[Nothing, Long]
        _       <- ZIO.foreach(1 to 100)(i => Nodes.addNode(NodeAddress(Array(1, 2, 3, 4), i)))
        node    = NodeAddress(Array(1, 1, 1, 1), 1111)
        start   <- currentTime(TimeUnit.MILLISECONDS)
        timeoutFiber <- SuspicionTimeout
                         .registerTimeout(node)(
                           currentTime(TimeUnit.MILLISECONDS).flatMap(current => promise.succeed(current - start))
                         )
                         .either
                         .fork
        _   <- TestClock.adjust(150.milliseconds)
        _   <- SuspicionTimeout.cancelTimeout(node)
        _   <- TestClock.adjust(50000.milliseconds)
        res <- timeoutFiber.join
      } yield assert(res)(isLeft(equalTo(SuspicionTimeoutCancelled(node))))
    }.provideCustomLayer(testLayer(1.second, 1, 2, 3))
  )

}
