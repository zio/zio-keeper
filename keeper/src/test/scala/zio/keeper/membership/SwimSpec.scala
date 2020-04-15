package zio.keeper.membership

import upickle.default._
import zio.clock.Clock
import zio.console.Console
import zio.keeper.discovery.{Discovery, TestDiscovery}
import zio.keeper.membership.swim.SWIM
import zio.logging.Logging.Logging
import zio.logging.{LogAnnotation, Logging, log}
import zio.stream.Sink
import zio.test.Assertion._
import zio.test.{DefaultRunnableSpec, assert, suite, testM}
import zio.{IO, Promise, UIO, ZIO, ZLayer, keeper}

object SwimSpec extends DefaultRunnableSpec {

  private case class MemberHolder[A](instance: Membership.Service[A], stop: UIO[Unit])

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

  private def member(port: Int) =
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
              .provideSomeLayer[TestDiscovery.TestDiscovery with Logging.Logging with Discovery with Clock](swim(port))
              .catchAll(err => start.fail(err))
              .fork
        cluster <- start.await
      } yield MemberHolder[EmptyProtocol](cluster, shutdown.succeed(()).ignore)

    }

  def spec =
    suite("cluster")(
      testM("all nodes should have references to each other") {
        for {
          member1 <- member(33331)
          member2 <- member(33332)
          _       <- member(33332)
          //we remove member 2 from discovery list to check if join broadcast works.
          _       <- TestDiscovery.removeMember(member2.instance.localMember)
          member3 <- member(33333)
          //we are waiting for events propagation to check nodes list
          _      <- member1.instance.events.run(Sink.collectAllN[MembershipEvent](2))
          _      <- member2.instance.events.run(Sink.collectAllN[MembershipEvent](2))
          _      <- member3.instance.events.run(Sink.collectAllN[MembershipEvent](2))
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
      }.provideSomeLayer[Logging.Logging with Clock](TestDiscovery.live) /*,
      testM("should receive notification") {
        for {
          member1     <- member(44331)
          member2     <- member(44332)
          member3     <- member(44333)
          node2       = member2.instance.localMember
          joinEvent   <- member1.instance.events.run(Sink.collectAllN[MembershipEvent](2))
          _           <- member2.stop
          leaveEvents <- member1.instance.events.run(Sink.collectAllN[MembershipEvent](3))
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
      }.provideSomeLayer[Logging.Logging with Clock](TestDiscovery.live)*/
    ).provideLayer(Clock.live ++ logging)
}
