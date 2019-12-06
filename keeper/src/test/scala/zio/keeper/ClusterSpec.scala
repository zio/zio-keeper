package zio.keeper

import zio._
import zio.duration._
import zio.keeper.discovery.TestDiscovery
import zio.keeper.membership.{ Member, Membership, SWIM }
import zio.logging.AbstractLogging
import zio.logging.slf4j.Slf4jLogger
import zio.macros.delegate.{ enrichWith, _ }
import zio.test.Assertion._
import zio.test.environment.Live
import zio.test.{ DefaultRunnableSpec, _ }

object ClusterSpec
    extends DefaultRunnableSpec({

      val loggingEnv = ZIO.environment[zio.ZEnv] @@ enrichWith[AbstractLogging[String]](
        new Slf4jLogger.Live {
          override def formatMessage(msg: String): ZIO[Any, Nothing, String] =
            ZIO.succeed(msg)
        }
      )

      val discoveryEnv = ZIO.environment[zio.ZEnv] @@
        enrichWithM[TestDiscovery](
          Ref
            .make(Set.empty[Member])
            .map(
              ref =>
                new TestDiscovery {
                  override def discover: TestDiscovery.Service[Any] = new TestDiscovery.Test(ref)
                }
            )
        )

      def tcpEnv =
        loggingEnv >>> ZIO
          .environment[zio.ZEnv with AbstractLogging[String]] @@ transport.tcp.withTcpTransport(10.seconds, 10.seconds)

      def dependencies =
        discoveryEnv @@
          enrichWithM(loggingEnv) @@
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

      trait ClusterHolder {
        def instance: Membership.Service[Any]
        def stop: UIO[Unit]
      }

      def cluster(port: Int) =
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
        } yield new ClusterHolder {
          def instance = cluster
          def stop     = shutdown.succeed(()).unit
        }

      suite("cluster")(
        testM("all nodes should have references to each other") {
          environment >>> Live.live(
            for {
              c1     <- cluster(3333)
              c2     <- cluster(3331)
              _      <- ZIO.accessM[TestDiscovery](d => c2.instance.localMember.flatMap(m => d.discover.removeMember(m)))
              c3     <- cluster(3332)
              nodes1 <- c1.instance.nodes
              nodes2 <- c2.instance.nodes
              nodes3 <- c3.instance.nodes
              node1  <- c1.instance.localMember.map(_.nodeId)
              node2  <- c2.instance.localMember.map(_.nodeId)
              node3  <- c3.instance.localMember.map(_.nodeId)
              _      <- c1.stop *> c2.stop *> c3.stop
            } yield assert(nodes1, equalTo(List(node2, node3))) &&
              assert(nodes2, equalTo(List(node1, node3))) &&
              assert(nodes3, equalTo(List(node1, node2)))
          )
        }
      )
    })
