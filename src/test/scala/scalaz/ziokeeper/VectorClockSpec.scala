package scalaz.ziokeeper

import scalaz.ziokeeper.VectorClock.VectorClockOrdering

class VectorClockSpec extends org.specs2.mutable.Specification {
  override def is = s2"""
    This specification is for Vector Clock

    Vector clock should
      increment local value even when member is not in map yet $e1
      increment local value when member is on the list $e2
      compare to other vector clock (BEFORE) $e3
      compare to other vector clock (AFTER) $e4
      compare to other vector clock (THE SAME) $e5
      compare to other vector clock (CONCURRENT) $e6
      merge with other vector clock $e7
  """

  def e1 = {
    //given
    val localId = Integer.MAX_VALUE
    val vc      = VectorClock.empty[Int]
    //when
    val incremented = vc.increment(localId).increment(localId)
    val current     = incremented.time(localId)
    //then
    current must_=== 2L
    VectorClock(Map(localId -> 2L)) must_=== incremented
  }

  def e2 = {
    //given
    val localId = Integer.MAX_VALUE
    val vc      = VectorClock(Map(localId -> 12L))
    //when
    val incremented = vc.increment(localId)
    //then
    VectorClock(Map(localId -> 13L)) must_=== incremented
  }

  def e3 = {
    //given
    val localId = Integer.MAX_VALUE
    val vc      = VectorClock.empty[Int]
    //when
    val result = vc.compare(vc.increment(localId))
    //then
    result must_=== VectorClockOrdering.Before
  }

  def e4 = {
    //given
    val localId = Integer.MAX_VALUE
    val vc      = VectorClock.empty[Int]
    //when
    val result = vc.increment(localId).compare(vc)
    //then
    result must_=== VectorClockOrdering.After
  }

  def e5 = {
    //given
    val localId = Integer.MAX_VALUE
    val vc      = VectorClock.empty[Int]
    //when
    val result = vc.increment(localId).compare(vc.increment(localId))
    //then
    result must_=== VectorClockOrdering.TheSame
  }

  def e6 = {
    //given
    val vc = VectorClock.empty[Int]
    //when
    val result =
      vc.increment(Integer.MAX_VALUE)
        .compare(
          vc.increment(Integer.MIN_VALUE)
        )
    //then
    result must_=== VectorClockOrdering.Concurrent
  }

  def e7 = {
    //given
    val left = VectorClock
      .empty[Int]
      .increment(1)
      .increment(1)
      .increment(2)
      .increment(3)
      .increment(200)
    val right = VectorClock
      .empty[Int]
      .increment(1)
      .increment(2)
      .increment(2)
      .increment(2)
      .increment(3)
      .increment(100)
    //when
    val result = left.merge(right)
    //then
    result must_=== VectorClock(Map(1 -> 2, 2 -> 3, 3 -> 1, 200 -> 1, 100 -> 1))
  }

}
