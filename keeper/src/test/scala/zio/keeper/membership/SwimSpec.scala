package zio.keeper.membership

import upickle.default._
import zio.clock.Clock
import zio.console.Console
import zio.keeper.discovery.{ Discovery, TestDiscovery }
import zio.keeper.membership.swim.SWIM
import zio.logging.Logging.Logging
import zio.logging.{ LogAnnotation, Logging, log }
import zio.stream.Sink
import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, assert, suite, testM }
import zio.{ Cause, Fiber, IO, Promise, Schedule, UIO, ZIO, ZLayer, keeper }
import zio.duration._
import zio.keeper.membership.swim.Nodes.NodeState
import zio.console._
import zio._

object SwimSpec extends DefaultRunnableSpec {

  private case class MemberHolder[A](instance: Membership.Service[A], stop: UIO[Unit]) {

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

  private def swim(port: Int): ZLayer[Discovery with Logging with Clock, keeper.Error, Membership[EmptyProtocol]] =
    SWIM.run[EmptyProtocol](port)

  private val newMember =
    TestDiscovery.nextPort
      .flatMap(
        port =>
          log.locally(LogAnnotation.Name(s"member-$port" :: Nil)) {
            for {
              start    <- Promise.make[zio.keeper.Error, Membership.Service[EmptyProtocol]]
              shutdown <- Promise.make[Nothing, Unit]
              _ <- ZIO
                    .accessM[Membership[EmptyProtocol] with TestDiscovery.TestDiscovery] { env =>
                      TestDiscovery.addMember(env.get.localMember) *>
                        start.succeed(env.get) *>
                        shutdown.await
                    }
                    .provideSomeLayer[TestDiscovery.TestDiscovery with Logging.Logging with Discovery with Clock](
                      swim(port)
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
                .flatMap(ZIO.foreach(_)(_.prettyPrintM.flatMap(putStrLn(_).provideLayer(ZEnv.live))))
                .delay(30.seconds)
                .uninterruptible
                .fork
          member1 <- newMember
          member2 <- newMember
          //we remove member 2 from discovery list to check if join broadcast works.
          _       <- TestDiscovery.removeMember(member2.instance.localMember)
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
                .flatMap(ZIO.foreach(_)(_.prettyPrintM.flatMap(putStrLn(_).provideLayer(ZEnv.live))))
                .delay(30.seconds)
                .uninterruptible
                .fork
          member1     <- newMember
          member2     <- newMember
          member3     <- newMember
          node2       = member2.instance.localMember
          joinEvent   <- member1.expectingMembershipEvents(2)
          _           <- member2.stop
          leaveEvents <- member1.expectingMembershipEvents(3)
          _           <- member1.stop
          _           <- member3.stop
        } yield assert(joinEvent)(
          hasSameElements(List(MembershipEvent.Join(node2), MembershipEvent.Join(member3.instance.localMember)))
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
