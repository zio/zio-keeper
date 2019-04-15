package scalaz.ziokeeper.crdt

import scalaz.ziokeeper.VectorClock

case class ORSet[A, B](values: Vector[ORSet.VersionedEntry[A, B]]) {

  def localRemove(elem: A): (Vector[VectorClock[B]], ORSet[A, B]) = {
    val (filtered, removedVcs) = this.values.foldLeft(
      (Vector.empty[ORSet.VersionedEntry[A, B]], Vector.empty[VectorClock[B]])
    ) {
      case ((filteredAcc, vcs), ORSet.VersionedEntry(`elem`, c)) => (filteredAcc, vcs :+ c)
      case ((filteredAcc, vcs), entry)                           => (filteredAcc :+ entry, vcs)
    }
    (removedVcs, ORSet[A, B](filtered))
  }

  def remove(elem: A, vcs: Vector[VectorClock[B]]): ORSet[A, B] =
    ORSet(this.values.filterNot {
      case ORSet.VersionedEntry(`elem`, v) => vcs.contains(v)
      case _                               => false
    })

  def add(value: A, vectorClock: VectorClock[B]): ORSet[A, B] =
    ORSet(this.values :+ ORSet.VersionedEntry[A, B](value, vectorClock))
}

object ORSet {
  def empty[A, B] = ORSet[A, B](Vector.empty)

  case class VersionedEntry[A, B](value: A, clock: VectorClock[B])
}
