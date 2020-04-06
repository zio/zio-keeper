package zio.keeper.membership.swim

import zio.stm.{TMap, TRef}
import zio.{Chunk, ZIO}

import scala.collection.mutable

class Broadcast(ref: TRef[mutable.PriorityQueue[Broadcast.Item]]) {

  //this should use some sort of priority queue

  def add(message: Message.Broadcast[Chunk[Byte]]): ZIO[Any, Nothing, Unit] =
    ref.update(q => {
      q.+=(Broadcast.Item(100, message.message))
      q
    }).commit

  def broadcast(currentMessageSize: Int): ZIO[Any, Nothing, List[Chunk[Byte]]] =
    ref.get.map{ q =>
      val all = q.dequeueAll.map(item => item.copy(resend = item.resend - 1))
      q ++= all.filter(_.resend > 0)
      all.map(_.chunk).toList
    }.commit

}
object Broadcast {
  final case class Item(resend: Int, chunk: Chunk[Byte])
  object Item {
    implicit val ordering = Ordering.by[Item, Int](_.resend)
  }
}
