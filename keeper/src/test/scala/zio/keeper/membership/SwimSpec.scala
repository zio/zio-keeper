package zio.keeper.membership

import upickle.default._
import zio.clock.Clock
import zio.config.Config
import zio.console.{ Console, _ }
import zio.duration._
import zio.keeper.discovery.{ Discovery, TestDiscovery }
import zio.keeper.membership.swim.Nodes.NodeState
import zio.keeper.membership.swim.{ SWIM, SwimConfig }
import zio.keeper.{ ByteCodec, TaggedCodec }
import zio.logging.{ LogAnnotation, Logging, log }
import zio.stream.Sink
import zio.test.Assertion._
import zio.test.{ assert, suite, testM }
import zio.{ Cause, Fiber, IO, Promise, Schedule, UIO, ZIO, keeper, _ }

//TODO disable since it hangs on CI
object SwimSpec {

  private case class MemberHolder[A](instance: SWIM.Service[A], stop: UIO[Unit]) {

    def expectingMembershipEvents(n: Long): ZIO[Clock, keeper.Error, List[MembershipEvent]] =
      instance.events
        .run(Sink.collectAllN[MembershipEvent](n))
        .timeout(30.seconds)
        .map(_.toList.flatten)
  }

  private val logging = (Console.live ++ Clock.live) >>> Logging.console((_, line) => line)

  sealed trait EmptyProtocol

  object EmptyProtocol {
    case object EmptyMessage extends EmptyProtocol

    implicit val taggedCodec = new TaggedCodec[EmptyProtocol] {
      override def tagOf(a: EmptyProtocol): Byte = 1

      override def codecFor(tag: Byte): IO[Unit, ByteCodec[EmptyProtocol]] =
        ZIO.succeed(ByteCodec.fromReadWriter(macroRW[EmptyProtocol]))
    }
  }

  private val newMember =
    TestDiscovery.nextPort
      .flatMap(
        port =>
          log.locally(LogAnnotation.Name(s"member-$port" :: Nil)) {
            for {
              start    <- Promise.make[zio.keeper.Error, SWIM.Service[EmptyProtocol]]
              shutdown <- Promise.make[Nothing, Unit]
              _ <- ZIO
                    .accessM[SWIM[EmptyProtocol] with TestDiscovery.TestDiscovery] { env =>
                      env.get.localMember.flatMap { localMember =>
                        TestDiscovery.addMember(localMember) *>
                          start.succeed(env.get) *>
                          shutdown.await
                      }
                    }
                    .provideSomeLayer[
                      Config[SwimConfig] with TestDiscovery.TestDiscovery with Logging with Discovery with Clock
                    ](
                      SWIM.live[EmptyProtocol]
                    )
                    .provideSomeLayer[TestDiscovery.TestDiscovery with Logging with Discovery with Clock](
                      Config.fromMap(Map("PORT" -> port.toString), SwimConfig.description).orDie
                    )
                    .catchAll(err => log.error("error starting member on: " + port, Cause.fail(err)) *> start.fail(err))
                    .fork
              cluster <- start.await
            } yield MemberHolder[EmptyProtocol](cluster, shutdown.succeed(()).ignore)
          }
      )
      .retry(Schedule.spaced(2.seconds))

  def spec =
    suite("cluster")(
      testM("all nodes should have references to each other") {
        for {
          _ <- Fiber.dumpAll
                .flatMap(ZIO.foreach(_)(_.prettyPrintM.flatMap(putStrLn(_))))
                .delay(30.seconds)
                .provideLayer(ZEnv.live)
                .uninterruptible
                .fork
          member1 <- newMember
          member2 <- newMember
          //we remove member 2 from discovery list to check if join broadcast works.
          _       <- member2.instance.localMember.flatMap(TestDiscovery.removeMember)
          member3 <- newMember
          //we are waiting for events propagation to check nodes list
          _      <- member1.expectingMembershipEvents(2)
          _      <- member2.expectingMembershipEvents(2)
          _      <- member3.expectingMembershipEvents(2)
          nodes1 <- member1.instance.nodes
          nodes2 <- member2.instance.nodes
          nodes3 <- member3.instance.nodes
          node1  = member1.instance.localMember
          node2  = member2.instance.localMember
          node3  = member3.instance.localMember
          _      <- member1.stop *> member2.stop *> member3.stop
        } yield assert(nodes1)(hasSameElements(List(node2, node3))) &&
          assert(nodes2)(hasSameElements(List(node1, node3))) &&
          assert(nodes3)(hasSameElements(List(node1, node2)))
      }.provideLayer(Clock.live ++ logging ++ (logging >>> TestDiscovery.live)),
      testM("should receive notification") {
        for {
          _ <- Fiber.dumpAll
                .flatMap(ZIO.foreach(_)(_.prettyPrintM.flatMap(putStrLn(_))))
                .delay(30.seconds)
                .provideLayer(ZEnv.live)
                .uninterruptible
                .fork
          member1     <- newMember
          member2     <- newMember
          member3     <- newMember
          node2       <- member2.instance.localMember
          node3       <- member3.instance.localMember
          joinEvent   <- member1.expectingMembershipEvents(2)
          _           <- member2.stop
          leaveEvents <- member1.expectingMembershipEvents(3)
          _           <- member1.stop
          _           <- member3.stop
        } yield assert(joinEvent)(
          hasSameElements(List(MembershipEvent.Join(node2), MembershipEvent.Join(node3)))
        ) &&
          assert(leaveEvents)(
            hasSameElements(
              List(
                MembershipEvent.NodeStateChanged(node2, NodeState.Healthy, NodeState.Unreachable),
                MembershipEvent.NodeStateChanged(node2, NodeState.Unreachable, NodeState.Suspicion),
                MembershipEvent.NodeStateChanged(node2, NodeState.Suspicion, NodeState.Death)
              )
            )
          )
      }.provideLayer(Clock.live ++ logging ++ (logging >>> TestDiscovery.live))
    )

}
