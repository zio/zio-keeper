package zio.keeper

import zio._
import zio.clock.Clock
import zio.console.Console
import zio.duration._
import zio.keeper.discovery.Discovery
import zio.keeper.transport.tcp
import zio.macros.delegate.{ enrichWith, _ }
import zio.random.Random
import zio.test.Assertion._
import zio.test.environment.{ Live, TestClock, TestConsole, TestRandom }
import zio.test.{ DefaultRunnableSpec, _ }

object ClusterSpec
    extends DefaultRunnableSpec({

      val withTransport = tcp.withTcpTransport(10.seconds, 10.seconds)

      trait TestDiscovery extends Discovery {
        override def discover: TestDiscovery.Service[Any]

      }
      object TestDiscovery {
        trait Service[R] extends Discovery.Service[R] {
          def addMember(m: Member): UIO[Unit]
          def removeMember(m: Member): UIO[Unit]
        }

        class Test(ref: Ref[Set[Member]]) extends Service[Any] {
          override val discoverNodes =
            for {
              members <- ref.get
              addrs   <- ZIO.collectAll(members.map(_.addr.socketAddress))
            } yield addrs.toSet

          def addMember(m: Member)    = ref.update(_ + m).unit
          def removeMember(m: Member) = ref.update(_ - m).unit
        }
      }

      val environment =
        for {
          transport  <- (ZIO.environment[TestClock with TestConsole with TestRandom] @@ withTransport)
          membersRef <- Ref.make(Set.empty[Member])
          discovery: TestDiscovery = new TestDiscovery {
            override def discover: TestDiscovery.Service[Any] = new TestDiscovery.Test(membersRef)
          }
          result <- ZIO.succeed(discovery) @@ enrichWith(transport)
          live <- (Live.live(ZIO.environment[Console with Clock with Random]) @@ enrichWith(discovery) @@ withTransport)
                   .flatMap(Live.make)
          env <- ZIO.succeed(live) @@ enrichWith(result)
        } yield env

      trait ClusterHolder {
        def instance: Cluster
        def stop: UIO[Unit]
      }

      def cluster(port: Int) =
        for {
          start         <- Promise.make[Nothing, Cluster]
          shutdown      <- Promise.make[Nothing, Unit]
          discoveryTest <- ZIO.environment[TestDiscovery]
          fiber <- Cluster
                    .join(port)
                    .use(
                      cluster =>
                        discoveryTest.discover.addMember(cluster.localMember) *>
                          start.succeed(cluster) *>
                          shutdown.await
                    )
                    .fork
          cluster <- start.await
        } yield new ClusterHolder {
          def instance = cluster
          def stop     = shutdown.succeed(()) *> fiber.interrupt.unit
        }

      suite("cluster")(
        testM("all nodes should have references to each other") {
          Ref.make(Set.empty[Member]).flatMap {
            membersRef =>
              environment >>> Live.live(
                for {
                  c1     <- cluster(3333)
                  c2     <- cluster(3331)
                  _      <- membersRef.update(_ - c2.instance.localMember)
                  c3     <- cluster(3332)
                  nodes1 <- c1.instance.nodes
                  nodes2 <- c2.instance.nodes
                  nodes3 <- c3.instance.nodes
                  node1  = c1.instance.localMember.nodeId
                  node2  = c2.instance.localMember.nodeId
                  node3  = c3.instance.localMember.nodeId
                  _      <- c1.stop *> c2.stop *> c3.stop
                } yield assert(nodes1, equalTo(List(node2, node3))) &&
                  assert(nodes2, equalTo(List(node1, node3))) &&
                  assert(nodes3, equalTo(List(node1, node2)))
              )
          }
        }
      )
    })
