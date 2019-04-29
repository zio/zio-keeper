package scalaz.ziokeeper.crdt

import org.specs2.mutable.Specification
import scalaz.ziokeeper.VectorClock

class ORSetSpec extends Specification {
  override def is =
    s2"""
    This specification is for ORSet

    ORSet should
      add element $e1
      remove element $e2
      not remove when element in not in $e3
    """

  def e1 = {
    //given
    val localId       = Integer.MAX_VALUE
    val vc            = VectorClock.empty[Int].increment(localId)
    val set           = ORSet.empty[Int, Int]
    val expectedValue = 100
    //when
    val result = set.add(expectedValue, vc)
    //then
    result must_=== ORSet(Vector(ORSet.VersionedEntry(expectedValue, vc)))
  }

  def e2 = {
    //given
    val localId       = Integer.MAX_VALUE
    val vc            = VectorClock.empty[Int].increment(localId)
    val expectedValue = 100
    val set           = ORSet.empty[Int, Int].add(expectedValue, vc)

    //when
    val (removedVcs, newORset) = set.localRemove(expectedValue)
    val result                 = set.remove(expectedValue, removedVcs)
    //then
    result must_=== ORSet.empty[Int, Int]
    newORset must_=== ORSet.empty[Int, Int]
    removedVcs must_=== Vector(vc)
  }

  def e3 = {
    //given
    val localId = Integer.MAX_VALUE
    val vc      = VectorClock.empty[Int].increment(localId)
    val set     = ORSet.empty[Int, Int].add(100, vc)

    //when
    val (removedVcs, newORset) = set.localRemove(200)
    //then
    newORset must_=== set
    removedVcs must_=== Vector()
  }
}
