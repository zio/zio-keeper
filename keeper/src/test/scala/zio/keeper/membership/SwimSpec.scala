package zio.keeper.membership

import zio.duration._
import zio.keeper.discovery.TestDiscovery
import zio.keeper.transport
import zio.logging.Logging
import zio.logging.slf4j.Slf4jLogger
import zio.macros.delegate._
import zio.stream.Sink
import zio.test.Assertion.equalTo
import zio.test.environment.Live
import zio.test.{ DefaultRunnableSpec, assert, suite, testM }
import zio.{ Promise, UIO, ZIO }

object SwimSpec
    extends DefaultRunnableSpec({

      val loggingEnv = ZIO.environment[zio.ZEnv] @@ enrichWith[Logging[String]](
        new Slf4jLogger.Live {
          override def formatMessage(msg: String): ZIO[Any, Nothing, String] =
            ZIO.succeed(msg)
        }
      )

      val discoveryEnv = ZIO.environment[zio.ZEnv] @@
        enrichWithM[TestDiscovery](
          TestDiscovery.test
        )

      def tcpEnv =
        loggingEnv >>> ZIO
          .environment[zio.ZEnv with Logging[String]] @@
          transport.tcp.withTcpTransport(10.seconds, 10.seconds)

      def dependencies =
        discoveryEnv @@
          enrichWithM(tcpEnv)

      val liveEnv =
        Live
          .live(
            ZIO.environment[zio.ZEnv] @@
              enrichWithM(dependencies)
          )
          .flatMap(Live.make)

      val environment =
        for {
          env <- dependencies
          r   <- liveEnv @@ enrichWith(env)
        } yield r

      trait MemberHolder {
        def instance: Membership.Service[Any]
        def stop: UIO[Unit]
      }

      def member(port: Int) =
        for {
          start         <- Promise.make[Nothing, Membership.Service[Any]]
          shutdown      <- Promise.make[Nothing, Unit]
          discoveryTest <- ZIO.environment[TestDiscovery]
          _ <- SWIM
                .join(port)
                .use(
                  cluster =>
                    cluster.membership.localMember.flatMap(
                      local =>
                        discoveryTest.discover.addMember(local) *>
                          start.succeed(cluster.membership) *>
                          shutdown.await
                    )
                )
                .fork
          cluster <- start.await
        } yield new MemberHolder {
          def instance = cluster
          def stop     = shutdown.succeed(()).unit
        }

      suite("cluster")(
        testM("all nodes should have references to each other") {
          environment >>> Live.live(
            for {
              member1 <- member(3333)
              member2 <- member(3331)
              _ <- ZIO.accessM[TestDiscovery](
                    d => member2.instance.localMember.flatMap(m => d.discover.removeMember(m))
                  )
              member3 <- member(3332)
              nodes1  <- member1.instance.nodes
              nodes2  <- member2.instance.nodes
              nodes3  <- member3.instance.nodes
              node1   <- member1.instance.localMember.map(_.nodeId)
              node2   <- member2.instance.localMember.map(_.nodeId)
              node3   <- member3.instance.localMember.map(_.nodeId)
              _       <- member1.stop *> member2.stop *> member3.stop
            } yield assert(nodes1, equalTo(List(node2, node3))) &&
              assert(nodes2, equalTo(List(node1, node3))) &&
              assert(nodes3, equalTo(List(node1, node2)))
          )
        },
        testM("should receive notification") {
          environment >>> Live.live(
            for {
              member1       <- member(4333)
              member1Events = member1.instance.events
              member2       <- member(4331)
              node2         <- member2.instance.localMember
              joinEvent     <- member1Events.run(Sink.await[MembershipEvent])
              _             <- member2.stop
              leaveEvents   <- member1Events.run(Sink.collectAllN[MembershipEvent](2))
              _             <- member1.stop
            } yield assert(joinEvent, equalTo(MembershipEvent.Join(node2))) &&
              assert(leaveEvents, equalTo(List(MembershipEvent.Unreachable(node2), MembershipEvent.Leave(node2))))
          )
        }
      )
    })
