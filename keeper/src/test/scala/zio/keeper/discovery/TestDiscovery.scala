package zio.keeper.discovery

import zio.keeper.membership.NodeAddress
import zio.logging.Logging.Logging
import zio.logging._
import zio._

object TestDiscovery {

  type TestDiscovery = Has[Service]

  def addMember(m: NodeAddress): URIO[TestDiscovery, Unit] =
    URIO.accessM[TestDiscovery](_.get.addMember(m))

  def removeMember(m: NodeAddress): URIO[TestDiscovery, Unit] =
    URIO.accessM[TestDiscovery](_.get.removeMember(m))

  val live: ZLayer[Logging, Nothing, Discovery with TestDiscovery] =
    ZLayer.fromEffectMany {
      ZIO.accessM[Logging] { logger =>
        logger.get.logger.info("creating test discovery") *>
          Ref
            .make(Set.empty[NodeAddress])
            .map(new Test(_, logger.get.logger))
            .map(test => Has.allOf[Discovery.Service, Service](test, test))
      }

    }

  trait Service extends Discovery.Service {
    def addMember(m: NodeAddress): UIO[Unit]
    def removeMember(m: NodeAddress): UIO[Unit]
  }

  private class Test(ref: Ref[Set[NodeAddress]], logger: Logger) extends Service {

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
  }
}
