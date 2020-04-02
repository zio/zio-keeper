package zio.keeper.membership

import zio.{ Promise, UIO, ZIO }
import zio.clock.Clock
import zio.duration._
import zio.keeper.Error
import zio.keeper.discovery.{ Discovery, TestDiscovery }
import zio.keeper.transport._
import zio.logging.Logging
import zio.random.Random
import zio.stream.Sink
import zio.test.DefaultRunnableSpec
import zio.test.{ assert, suite, testM }
import zio.test.Assertion.equalTo

object SwimSpec extends DefaultRunnableSpec {

  private trait MemberHolder {
    def instance: Membership.Service
    def stop: UIO[Unit]
  }

  private val environment = {
    val clockAndLogging = Clock.live ++ Logging.ignore
    val transport       = clockAndLogging >>> tcp.live(10.seconds, 10.seconds)
    clockAndLogging ++ Random.live ++ transport ++ TestDiscovery.live
  }

  private def member(port: Int): ZIO[
    Logging.Logging with Clock with Random with Transport with Discovery with TestDiscovery.TestDiscovery,
    Error,
    MemberHolder
  ] =
    for {
      start    <- Promise.make[Nothing, Membership.Service]
      shutdown <- Promise.make[Nothing, Unit]
      _ <- SWIM
            .join(port)
            .build
            .map(_.get)
            .use { cluster =>
              cluster.localMember.flatMap { local =>
                TestDiscovery.addMember(local) *>
                  start.succeed(cluster) *>
                  shutdown.await
              }
            }
            .fork
      cluster <- start.await
    } yield new MemberHolder {
      val instance = cluster
      val stop     = shutdown.succeed(()).unit
    }

  def spec =
    suite("cluster")(
      testM("all nodes should have references to each other") {
        for {
          member1 <- member(33333)
          member2 <- member(33331)
          _       <- member2.instance.localMember.flatMap(m => TestDiscovery.removeMember(m))
          member3 <- member(33332)
          _       <- member1.instance.events.run(Sink.collectAllN[MembershipEvent](2))
          nodes1  <- member1.instance.nodes
          nodes2  <- member2.instance.nodes
          nodes3  <- member3.instance.nodes
          node1   <- member1.instance.localMember.map(_.nodeId)
          node2   <- member2.instance.localMember.map(_.nodeId)
          node3   <- member3.instance.localMember.map(_.nodeId)
          _       <- member1.stop *> member2.stop *> member3.stop
        } yield assert(nodes1)(equalTo(List(node2, node3))) &&
          assert(nodes2)(equalTo(List(node1, node3))) &&
          assert(nodes3)(equalTo(List(node1, node2)))
      },
      testM("should receive notification") {
        for {
          member1       <- member(44333)
          member1Events = member1.instance.events
          member2       <- member(44331)
          node2         <- member2.instance.localMember
          joinEvent     <- member1Events.run(Sink.await[MembershipEvent])
          _             <- member2.stop
          leaveEvents   <- member1Events.run(Sink.collectAllN[MembershipEvent](2))
          _             <- member1.stop
        } yield assert(joinEvent)(equalTo(MembershipEvent.Join(node2))) &&
          assert(leaveEvents)(equalTo(List(MembershipEvent.Unreachable(node2), MembershipEvent.Leave(node2))))
      }
    ).provideCustomLayer(environment)
}
