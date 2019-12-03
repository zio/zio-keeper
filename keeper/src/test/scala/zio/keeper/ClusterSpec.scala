package zio.keeper

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
import zio.{IO, Promise, Ref, ZIO}

object ClusterSpec extends DefaultRunnableSpec({

  val withTransport = tcp.withTcpTransport(10.seconds, 10.seconds)

  def environment(clusterMembers: Ref[Set[Member]]) =
    for {
      transport <- (ZIO.environment[TestClock with TestConsole with TestRandom] @@ withTransport)
      config: Credentials with Discovery = new Credentials with Discovery {
        override val discover: IO[Error, Set[SocketAddress]] =
        for {
          members <- clusterMembers.get
          addrs <- ZIO.collectAll(members.map(_.addr.socketAddress))
        } yield addrs.toSet
      }
      result <- ZIO.succeed(config) @@ enrichWith(transport)
      result <- (Live.live(ZIO.environment[Console with Clock with Random]) @@ enrichWith(result)).flatMap(Live.make)
      env  <- ZIO.succeed(result) @@ enrichWith(config)
    } yield env

  def createCluster = Cluster
    .join(0)


  testM("blah"){
    Ref.make(Set.empty[Member]).flatMap(membersRef =>
      environment(membersRef) >>> Live.live(
                (for {
                  shutdown <- Promise.make[Nothing, Unit]
                  _ <- createCluster.use(c => membersRef.update(_ + c.localMember) *> shutdown.await)
                  _ <- shutdown.succeed(())
                } yield assert("ZIO.unit", equalTo("a")))
      ))


    

//    assertM(ZIO.unit, equalTo("ss"))
  }
})
