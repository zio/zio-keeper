package zio.keeper.discovery

import zio._
import zio.keeper.NodeAddress
import zio.logging.Logging.Logging
import zio.logging._

object TestDiscovery {

  type TestDiscovery = Has[Service]

  def addMember(m: NodeAddress): URIO[TestDiscovery, Unit] =
    URIO.accessM[TestDiscovery](_.get.addMember(m))

  def removeMember(m: NodeAddress): URIO[TestDiscovery, Unit] =
    URIO.accessM[TestDiscovery](_.get.removeMember(m))

  val nextPort: URIO[TestDiscovery, Int] =
    URIO.accessM[TestDiscovery](_.get.nextPortNumber)

  val live: ZLayer[Logging, Nothing, Discovery with TestDiscovery] =
    ZLayer.fromEffectMany {
      for {
        logger <- ZIO.environment[Logging]
        _      <- logger.get.logger.info("creating test discovery")
        nodes  <- Ref.make(Set.empty[NodeAddress])
        ports  <- Ref.make(10000)
        test   = new Test(nodes, ports, logger.get.logger)
      } yield Has.allOf[Discovery.Service, Service](test, test)
    }

  trait Service extends Discovery.Service {
    def addMember(m: NodeAddress): UIO[Unit]
    def removeMember(m: NodeAddress): UIO[Unit]
    def nextPortNumber: UIO[Int]
  }

  private class Test(ref: Ref[Set[NodeAddress]], port: Ref[Int], logger: Logger) extends Service {

    val discoverNodes =
      for {
        members <- ref.get
        addrs   <- IO.collectAll(members.map(_.socketAddress))
      } yield addrs.toSet

    def addMember(m: NodeAddress) =
      logger.info("adding node: " + m) *>
        ref.update(_ + m).unit

    def removeMember(m: NodeAddress) =
      logger.info("removing node: " + m) *>
        ref.update(_ - m).unit

    def nextPortNumber: UIO[Int] =
      port.updateAndGet(_ + 1)
  }
}
