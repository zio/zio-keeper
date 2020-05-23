package zio.keeper.membership

import upickle.default._
import zio._
import zio.clock.Clock
import zio.config.Config
import zio.console.Console
import zio.duration._
import zio.keeper.discovery.{ Discovery, TestDiscovery }
import zio.keeper.membership.swim.{ SWIM, SwimConfig }
import zio.keeper.ByteCodec
import zio.logging.{ LogAnnotation, Logging, log }
import zio.stream.Sink
import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, TestAspect, assert, suite, testM }

//TODO disable since it hangs on CI
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

    case object EmptyMessage extends EmptyProtocol {

      implicit val codec: ByteCodec[EmptyMessage.type] =
        ByteCodec.fromReadWriter(macroRW[EmptyMessage.type])
    }

    implicit val codec: ByteCodec[EmptyProtocol] =
      ByteCodec.tagged[EmptyProtocol][
        EmptyMessage.type
      ]
  }

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
          node1  <- member1.instance.localMember
          node2  <- member2.instance.localMember
          node3  <- member3.instance.localMember
          _      <- member1.stop *> member2.stop *> member3.stop
        } yield assert(nodes1)(hasSameElements(List(node2, node3))) &&
          assert(nodes2)(hasSameElements(List(node1, node3))) &&
          assert(nodes3)(hasSameElements(List(node1, node2)))
      }.provideLayer(Clock.live ++ logging ++ (logging >>> TestDiscovery.live)),
      testM("should receive notification") {
        for {
          member1     <- newMember
          member2     <- newMember
          member3     <- newMember
          node2       <- member2.instance.localMember
          node3       <- member3.instance.localMember
          joinEvent   <- member1.expectingMembershipEvents(2)
          _           <- member2.stop
          leaveEvents <- member1.expectingMembershipEvents(1)
          _           <- member1.stop
          _           <- member3.stop
        } yield assert(joinEvent)(
          hasSameElements(List(MembershipEvent.Join(node2), MembershipEvent.Join(node3)))
        ) &&
          assert(leaveEvents)(
            hasSameElements(
              List(
                MembershipEvent.Leave(node2)
              )
            )
          )
      }.provideLayer(Clock.live ++ logging ++ (logging >>> TestDiscovery.live))
    ) @@ TestAspect.sequential

}
