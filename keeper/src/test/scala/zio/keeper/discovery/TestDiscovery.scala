package zio.keeper.discovery

import zio.keeper.membership.NodeAddress
import zio.{Has, IO, Layer, Ref, UIO, URIO, ZLayer}

object TestDiscovery {

  type TestDiscovery = Has[Service]

  def addMember(m: NodeAddress): URIO[TestDiscovery, Unit] =
    URIO.accessM[TestDiscovery](_.get.addMember(m))

  def removeMember(m: NodeAddress): URIO[TestDiscovery, Unit] =
    URIO.accessM[TestDiscovery](_.get.removeMember(m))

  def live: Layer[Nothing, Discovery with TestDiscovery] =
    ZLayer.fromEffectMany {
      Ref
        .make(Set.empty[NodeAddress])
        .map(new Test(_))
        .map(test => Has.allOf[Discovery.Service, Service](test, test))
    }

  trait Service extends Discovery.Service {
    def addMember(m: NodeAddress): UIO[Unit]
    def removeMember(m: NodeAddress): UIO[Unit]
  }

  private class Test(ref: Ref[Set[NodeAddress]]) extends Service {

    val discoverNodes =
      for {
        members <- ref.get
        addrs   <- IO.collectAll(members.map(_.socketAddress))
      } yield addrs.toSet

    def addMember(m: NodeAddress) = ref.update(_ + m).unit

    def removeMember(m: NodeAddress) = ref.update(_ - m).unit
  }
}
