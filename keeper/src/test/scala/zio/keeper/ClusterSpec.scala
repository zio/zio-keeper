package zio.keeper

import zio._
import zio.clock.Clock
import zio.console.Console
import zio.duration._
import zio.keeper.Cluster.Credentials
import zio.keeper.discovery.Discovery
import zio.keeper.transport.tcp
import zio.macros.delegate.{enrichWith, _}
import zio.nio.SocketAddress
import zio.random.Random
import zio.test.Assertion._
import zio.test.environment.{Live, TestClock, TestConsole, TestRandom}
import zio.test.{DefaultRunnableSpec, _}

object ClusterSpec
    extends DefaultRunnableSpec({

      val withTransport = tcp.withTcpTransport(10.seconds, 10.seconds)

      case class TestDiscovery(ref: Ref[Set[Member]]) extends Discovery {
        override val discover: ZIO[Console, Error, Set[SocketAddress]] =
          for {
            members <- ref.get
            addrs   <- ZIO.collectAll(members.map(_.addr.socketAddress))
          } yield addrs.toSet

        def add(m: Member) = ref.update(_ + m)
        def remove(m: Member) = ref.update(_ - m)
      }

      def environment(clusterMembers: Ref[Set[Member]]) =
        for {
          transport <- (ZIO.environment[TestClock with TestConsole with TestRandom] @@ withTransport)
          discovery <- Ref.make(Set.empty[Member]).map(ref => TestDiscovery(ref))
          config: Credentials with Discovery = new Credentials with Discovery {
            override val discover: IO[Error, Set[SocketAddress]] =
              for {
                members <- clusterMembers.get
                addrs   <- ZIO.collectAll(members.map(_.addr.socketAddress))
              } yield addrs.toSet
          }
          result <- ZIO.succeed(config) @@ enrichWith(transport)
          live <- (Live.live(ZIO.environment[Console with Clock with Random]) @@ enrichWith(config) @@ withTransport)
                   .flatMap(Live.make)
          env <- ZIO.succeed(live) @@ enrichWith(result)
        } yield env

      trait ClusterHolder {
        def instance: Cluster
        def stop: UIO[Unit]
      }

      def cluster(port: Int, membersRef: Ref[Set[Member]]) =
        for {
          start    <- Promise.make[Nothing, Cluster]
          shutdown <- Promise.make[Nothing, Unit]
          _ <- Cluster
                .join(port)
                .use(
                  cluster =>
                    membersRef.update(_ + cluster.localMember) *>
                      start.succeed(cluster) *>
                      shutdown.await
                )
                .fork
          cluster <- start.await
        } yield new ClusterHolder {
          def instance = cluster
          def stop     = shutdown.succeed(()).unit
        }

      suite("cluster")(
        testM("all nodes should have references to each other") {
          Ref.make(Set.empty[Member]).flatMap {
            membersRef =>
              environment(membersRef) >>> Live.live(
                for {
                  c1     <- cluster(3333, membersRef)
                  c2     <- cluster(3331, membersRef)
                  _      <- membersRef.update(_ - c2.instance.localMember)
                  c3     <- cluster(3332, membersRef)
                  nodes1 <- c1.instance.nodes
                  nodes2 <- c2.instance.nodes
                  nodes3 <- c3.instance.nodes
                  node1  = c1.instance.localMember.nodeId
                  node2  = c2.instance.localMember.nodeId
                  node3  = c3.instance.localMember.nodeId
                } yield assert(nodes1, equalTo(List(node2, node3))) &&
                  assert(nodes2, equalTo(List(node1, node3))) &&
                  assert(nodes3, equalTo(List(node1, node2)))
              )
          }
        },
        testM("all nodes should have references to each other") {
          Ref.make(Set.empty[Member]).flatMap {
            membersRef =>
              environment(membersRef) >>> Live.live(
                for {
                  c1     <- cluster(4333, membersRef)
                  c2     <- cluster(4331, membersRef)
                  onlyC2 <- Ref.make(Set(c2.instance.localMember))
                  c3     <- cluster(4332, onlyC2)
                  nodes1 <- c1.instance.nodes
                  nodes2 <- c2.instance.nodes
                  nodes3 <- c3.instance.nodes
                  node1  = c1.instance.localMember.nodeId
                  node2  = c2.instance.localMember.nodeId
                  node3  = c3.instance.localMember.nodeId
                } yield assert(nodes1, equalTo(List(node2, node3))) &&
                  assert(nodes2, equalTo(List(node1, node3))) &&
                  assert(nodes3, equalTo(List(node1, node2)))
              )
          }
        }
      )
    })
