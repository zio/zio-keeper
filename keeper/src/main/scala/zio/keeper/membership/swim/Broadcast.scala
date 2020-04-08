package zio.keeper.membership.swim

import zio.stm.{TMap, TRef}
import zio.{Chunk, ZIO}

import scala.collection.mutable

class Broadcast(ref: TRef[mutable.PriorityQueue[Broadcast.Item]], sequenceId: TRef[Int]) {

  def add(message: Message.Broadcast[Chunk[Byte]]): ZIO[Any, Nothing, Unit] =
    sequenceId.getAndUpdate(_ + 1).flatMap(seqId =>
      ref.update(q => {
        q.+=(Broadcast.Item(seqId, 100, message.message))
        q
      })
    ).commit

  def broadcast(currentMessageSize: Int): ZIO[Any, Nothing, List[Chunk[Byte]]] =
    ref.modify{ q =>
      val all = q.dequeueAll.map(item => item.copy(resend = item.resend - 1))
      q ++= all.filter(_.resend > 0)
      (all.map(_.chunk).toList, q)
    }.commit

}
object Broadcast {
  final case class Item(seqId: Int, resend: Int, chunk: Chunk[Byte])
  object Item {
    implicit val ordering = Ordering.by[Item, Int](_.resend)
  }
}
