package zio.keeper.membership

import upickle.default._
import zio.clock.Clock
import zio.console.Console
import zio.keeper.Error
import zio.keeper.discovery.{Discovery, TestDiscovery}
import zio.keeper.membership.swim.SWIM
import zio.logging.Logging
import zio.random.Random
import zio.stream.Sink
import zio.test.Assertion._
import zio.test.{DefaultRunnableSpec, assert, suite, testM}
import zio.{IO, Promise, UIO, ZIO}

object SwimSpec extends DefaultRunnableSpec {

  private trait MemberHolder[A] {
    def instance: Membership.Service[A]
    def stop: UIO[Unit]
  }

  private val environment = {
    val clockAndLogging = Console.live ++ Clock.live ++ Logging.console((_, line) => line)
    clockAndLogging ++ Random.live ++ TestDiscovery.live
  }

  sealed trait EmptyProtocol
  object EmptyProtocol {
    case object EmptyMessage extends EmptyProtocol
    implicit val taggedCodec = new TaggedCodec[EmptyProtocol] {
      override def tagOf(a: EmptyProtocol): Byte = 1

      override def codecFor(tag: Byte): IO[Unit, ByteCodec[EmptyProtocol]] =
        ZIO.succeed(ByteCodec.fromReadWriter(macroRW[EmptyProtocol]))
    }
  }

  private def member(port: Int): ZIO[
    Logging.Logging with Clock with Random  with Discovery with TestDiscovery.TestDiscovery,
    Error,
    MemberHolder[EmptyProtocol]
  ] =
    for {
      start    <- Promise.make[Nothing, Membership.Service[EmptyProtocol]]
      shutdown <- Promise.make[Nothing, Unit]
      _ <- SWIM
            .run[EmptyProtocol](port)
            .build
            .map(_.get)
            .use { cluster =>
              TestDiscovery.addMember(cluster.localMember) *>
                start.succeed(cluster) *>
                shutdown.await
            }
            .fork
      cluster <- start.await
    } yield new MemberHolder[EmptyProtocol] {
      val instance = cluster
      val stop     = shutdown.succeed(()).unit
    }

  def spec =
    suite("cluster")(
      testM("all nodes should have references to each other") {
        for {
          member1 <- member(33333)
          member2 <- member(33331)
          _       <- TestDiscovery.removeMember(member2.instance.localMember)
          member3 <- member(33332)
          _       <- member1.instance.events.run(Sink.collectAllN[MembershipEvent](2))
          nodes1  <- member1.instance.nodes
          nodes2  <- member2.instance.nodes
          nodes3  <- member3.instance.nodes
          node1   = member1.instance.localMember
          node2   = member2.instance.localMember
          node3   = member3.instance.localMember
          _       <- member1.stop *> member2.stop *> member3.stop
        } yield assert(nodes1)(hasSameElements(List(node2, node3))) &&
          assert(nodes2)(hasSameElements(List(node1, node3))) &&
          assert(nodes3)(hasSameElements(List(node1, node2)))
      }/*,
      testM("should receive notification") {
        for {
          member1       <- member(44333)
          member1Events = member1.instance.events
          member2       <- member(44331)
          node2         = member2.instance.localMember
          joinEvent     <- member1Events.run(Sink.await[MembershipEvent])
          _             <- member2.stop
          leaveEvents   <- member1Events.run(Sink.collectAllN[MembershipEvent](2))
          _             <- member1.stop
        } yield assert(joinEvent)(equalTo(MembershipEvent.Join(node2))) &&
          assert(leaveEvents)(equalTo(
            List(
              MembershipEvent.NodeStateChanged(node2, NodeState.Healthy, NodeState.Unreachable),
              MembershipEvent.NodeStateChanged(node2, NodeState.Unreachable, NodeState.Death)
            )
          ))
      }*/
    ).provideCustomLayer(environment)
}
