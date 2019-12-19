package zio.keeper.discovery

import zio.{ Ref, UIO, ZIO }
import zio.keeper.membership.Member

trait TestDiscovery extends Discovery {
  override val discover: TestDiscovery.Service[Any]
}

object TestDiscovery {

  def test =
    Ref
      .make(Set.empty[Member])
      .map(
        ref =>
          new TestDiscovery {
            override val discover: Service[Any] = new Test(ref)
          }
      )

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
