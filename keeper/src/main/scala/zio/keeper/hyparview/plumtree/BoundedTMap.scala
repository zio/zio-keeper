package zio.keeper.hyparview.plumtree

import zio._
import zio.stm._

import scala.collection.immutable.ListMap

// TODO: optimize this with native STM implementation
final class BoundedTMap[K, V] private[BoundedTMap] (val ref: TRef[ListMap[K, V]], val capacity: Int) {

  def add(key: K, value: V): USTM[Boolean] =
    ref.modify { seenBefore =>
      val result = !seenBefore.contains(key)

      val next = (result, seenBefore.size >= capacity) match {
        case (true, true)  => (seenBefore - seenBefore.head._1) + (key -> value)
        case (true, false) => seenBefore + (key -> value)
        case _             => seenBefore
      }
      (result, next)
    }

  def contains(key: K): USTM[Boolean] =
    ref.get.map(_.contains(key))

  def get(key: K): USTM[Option[V]] =
    ref.get.map(_.get(key))

}

object BoundedTMap {

  def make[K, V](capacity: Int): UIO[BoundedTMap[K, V]] =
    if (capacity <= 0) ZIO.dieMessage("Capacity must be a positive number")
    else TRef.make(ListMap.empty[K, V]).map(new BoundedTMap(_, capacity)).commit

}
