package zio.keeper.discovery

import zio.keeper.membership.Member
import zio.{ Has, IO, Layer, Ref, UIO, URIO, ZLayer }

object TestDiscovery {

  type TestDiscovery = Has[Service]

  def addMember(m: Member): URIO[TestDiscovery, Unit] =
    URIO.accessM[TestDiscovery](_.get.addMember(m))

  def removeMember(m: Member): URIO[TestDiscovery, Unit] =
    URIO.accessM[TestDiscovery](_.get.removeMember(m))

  def live: Layer[Nothing, Discovery with TestDiscovery] =
    ZLayer.fromEffectMany {
      Ref
        .make(Set.empty[Member])
        .map(new Test(_))
        .map(test => Has.allOf[Discovery.Service, Service](test, test))
    }

  trait Service extends Discovery.Service {
    def addMember(m: Member): UIO[Unit]
    def removeMember(m: Member): UIO[Unit]
  }

  private class Test(ref: Ref[Set[Member]]) extends Service {

    val discoverNodes =
      for {
        members <- ref.get
        addrs   <- IO.collectAll(members.map(_.addr.socketAddress))
      } yield addrs.toSet

    def addMember(m: Member) = ref.update(_ + m).unit

    def removeMember(m: Member) = ref.update(_ - m).unit
  }
}
