//package zio.keeper
//
//import zio._
//import zio.clock.Clock
//import zio.duration._
//import zio.keeper.discovery.Discovery
//import zio.keeper.membership.{Member, Membership, SWIM}
//import zio.keeper.transport.tcp
//import zio.logging.AbstractLogging
//import zio.logging.slf4j.{Logging, LoggingFormat, Slf4jLogger}
//import zio.macros.delegate.{enrichWith, _}
//import zio.random.Random
//import zio.test.Assertion._
//import zio.test.environment.{Live, TestClock, TestConsole, TestRandom}
//import zio.test.{DefaultRunnableSpec, _}
//
//object ClusterSpec
//    extends DefaultRunnableSpec({
//
//      val withTransport = tcp.withTcpTransport(10.seconds, 10.seconds)
//
//      trait TestDiscovery extends Discovery {
//        override def discover: TestDiscovery.Service[Any]
//
//      }
//      object TestDiscovery {
//        trait Service[R] extends Discovery.Service[R] {
//          def addMember(m: Member): UIO[Unit]
//          def removeMember(m: Member): UIO[Unit]
//        }
//
//        class Test(ref: Ref[Set[Member]]) extends Service[Any] {
//          override val discoverNodes =
//            for {
//              members <- ref.get
//              addrs   <- ZIO.collectAll(members.map(_.addr.socketAddress))
//            } yield addrs.toSet
//
//          def addMember(m: Member)    = ref.update(_ + m).unit
//          def removeMember(m: Member) = ref.update(_ - m).unit
//        }
//      }
//
//      val environment =
//        for {
//          logging <- ZIO.succeed[Logging](new Logging {
//            override def logging: AbstractLogging.Service[Any, String] = new Slf4jLogger {
//              override val slf4jMessageFormat: LoggingFormat = new LoggingFormat {
//                override def format(message: String): ZIO[Any, Nothing, String] =
//                  ZIO.succeed(message)
//              }
//            }
//          })
//          transport  <- (ZIO.environment[TestClock with TestRandom with Logging] @@ withTransport)
//          transport1  <- (ZIO.environment[Clock with Random with Logging] @@ withTransport)
//          membersRef <- Ref.make(Set.empty[Member])
//          discovery: TestDiscovery = new TestDiscovery {
//            override def discover: TestDiscovery.Service[Any] = new TestDiscovery.Test(membersRef)
//          }
//
//          result <- ZIO.succeed(discovery) @@ enrichWith(transport) @@ enrichWith(logging)
//          live <- (Live.live(ZIO.environment[Clock with Random]) @@ enrichWith(discovery) @@ enrichWith(transport1))
//                   .flatMap(Live.make)
//          env <- ZIO.succeed(live) @@ enrichWith(result)
//        } yield env
//
//      trait ClusterHolder {
//        def instance: Membership.Service[Any]
//        def stop: UIO[Unit]
//      }
//
//      def cluster(port: Int) =
//        for {
//          start         <- Promise.make[Nothing, Membership.Service[Any]]
//          shutdown      <- Promise.make[Nothing, Unit]
//          discoveryTest <- ZIO.environment[TestDiscovery]
//          _ <- SWIM
//                .join(port)
//                .use(
//                  cluster =>
//                    cluster.membership.localMember.flatMap(
//                      local =>
//                        discoveryTest.discover.addMember(local) *>
//                          start.succeed(cluster.membership) *>
//                          shutdown.await
//                    )
//                )
//                .fork
//          cluster <- start.await
//        } yield new ClusterHolder {
//          def instance = cluster
//          def stop     = shutdown.succeed(()).unit
//        }
//
//      suite("cluster")(
//        testM("all nodes should have references to each other") {
//          RefM.make(Set.empty[Member]).flatMap {
//            membersRef =>
//              environment >>> Live.live(
//                for {
//                  c1     <- cluster(3333)
//                  c2     <- cluster(3331)
//                  _      <- membersRef.update(old => c2.instance.localMember.map(old - _))
//                  c3     <- cluster(3332)
//                  nodes1 <- c1.instance.nodes
//                  nodes2 <- c2.instance.nodes
//                  nodes3 <- c3.instance.nodes
//                  node1  <- c1.instance.localMember.map(_.nodeId)
//                  node2  <- c2.instance.localMember.map(_.nodeId)
//                  node3  <- c3.instance.localMember.map(_.nodeId)
//                  _      <- c1.stop *> c2.stop *> c3.stop
//                } yield assert(nodes1, equalTo(List(node2, node3))) &&
//                  assert(nodes2, equalTo(List(node1, node3))) &&
//                  assert(nodes3, equalTo(List(node1, node2)))
//              )
//          }
//        }
//      )
//    })
