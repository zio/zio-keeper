package scalaz.ziokeeper

import scala.annotation.tailrec
import VectorClock._
import scalaz.ziokeeper.VectorClock.VectorClockOrdering.{ After, Before, Concurrent, TheSame }

/**
 * Implements an algorithm for generating a partial ordering of events in a distributed system and detecting causality violations.
 *
 * In the time-stamping system, each process has a VectorClock that is maintained as follows:
 * - increment local counter in order to track progress whatever performs actions(e.g. add, remove, modify)
 * as well as when you receive and send messages from/to other members of the cluster.
 * - when a cluster member sends a message, it attaches to it the current value of VectorClock
 * - whenever we receive event from other cluster members we takes max between position stored in local vector clock about positions of others nodes and
 *  respective positions from incoming message.
 *
 * NOTE: Keep in mind that vector clock provides partial order not total order so there will still appear
 * situations that two events in the distributed system with Vector Clock timestamping be concurrent to each other.
 *
 */
case class VectorClock[A](value: Map[A, Long]) extends AnyVal {
  def time(mid: A): Long = value.getOrElse(mid, 0L)

  def increment(mid: A): VectorClock[A] =
    VectorClock(this.value.updated(mid, time(mid) + 1))

  def compare(right: VectorClock[A]): VectorClockOrdering = {
    val leftWithDefault  = this.value.withDefaultValue(0L)
    val rightWithDefault = right.value.withDefaultValue(0L)

    @tailrec
    def loop(keys: List[A], leftBigger: Boolean, rightBigger: Boolean): (Boolean, Boolean) =
      keys match {
        case _ if leftBigger && rightBigger => (leftBigger, rightBigger)
        case Nil                            => (leftBigger, rightBigger)
        case h :: t =>
          val diff = leftWithDefault(h) - rightWithDefault(h)
          loop(t, leftBigger || diff > 0, rightBigger || diff < 0)
      }

    val keys = leftWithDefault.keySet.union(rightWithDefault.keySet)

    loop(keys.toList, leftBigger = false, rightBigger = false) match {
      case (false, false) => TheSame
      case (true, true)   => Concurrent
      case (true, false)  => After
      case (false, true)  => Before
    }
  }

  def mergeWith(right: VectorClock[A])(f: (Long, Long) => Long): VectorClock[A] = {
    val leftWithDefault  = this.value.withDefaultValue(0L)
    val rightWithDefault = right.value.withDefaultValue(0L)

    VectorClock(
      leftWithDefault.keySet
        .union(rightWithDefault.keySet)
        .foldLeft(Map.empty[A, Long]) {
          case (acc, key) =>
            acc + (key -> f(leftWithDefault(key), rightWithDefault(key)))
        }
    )
  }

  def merge(right: VectorClock[A]): VectorClock[A] =
    mergeWith(right)(_.max(_))
}

object VectorClock {
  def empty[A] = VectorClock[A](value = Map.empty)

  /**
   * Represents Partial Ordering of the Vector clock.
   *
   * When we have cluster with three member with ids 111, 222, 333.
   * Vector clocks initially will look like below on all nodes.
   * (111 -> 0, 222 -> 0, 333 -> 0)
   *
   * At this point the are all TheSame.
   *
   * imagine that 111 and 222 perform add action
   *
   * 111 = (111 -> 1, 222 -> 0, 333 -> 0)
   * 222 = (111 -> 0, 222 -> 1, 333 -> 0)
   * 333 = (111 -> 0, 222 -> 0, 333 -> 0)
   *
   * 111 and 222 are Concurrent because you cannot say which happen first.
   *
   */
  sealed trait VectorClockOrdering

  object VectorClockOrdering {
    case object TheSame extends VectorClockOrdering

    case object Before extends VectorClockOrdering

    case object After extends VectorClockOrdering

    case object Concurrent extends VectorClockOrdering
  }
}
