package zio.keeper.membership.swim

import zio.keeper.membership.swim.Broadcast.Item
import zio.stm.TRef
import zio.{ Chunk, ZIO }

import scala.collection.immutable.TreeSet

class Broadcast(ref: TRef[TreeSet[Item]], sequenceId: TRef[Int], messageOverhead: Int, messageLimit: Int) {

  def add(message: Message.Broadcast[Chunk[Byte]]): ZIO[Any, Nothing, Unit] =
    sequenceId
      .getAndUpdate(_ + 1)
      .flatMap[Any, Nothing, Unit](
        seqId =>
          ref.update(
            items => items + Item(seqId, 10 /* this should calculated based of num of nodes */, message.message)
          )
      )
      .commit

  def broadcast(currentMessageSize: Int): UIO[List[Chunk[Byte]]] =
    ref.modify { items =>
      val (toSend, toReschedule, _) = items.foldRight((Vector.empty[Item], Vector.empty[Item], currentMessageSize)) {
        case (item, (toSend, toReschedule, size))
            if item.chunk.size + messageOverhead <= messageLimit - currentMessageSize =>
          (toSend :+ item, toReschedule, size + item.chunk.size + messageOverhead)
      }

      val newValue  = TreeSet.empty[Item](Item.ordering) ++ toSend.map(item => item.copy(resend = item.resend - 1)) ++ toReschedule
      val broadcast = toSend.map(item => item.chunk).toList
      (broadcast, newValue)
    }.commit

}

object Broadcast {

  def make(mtu: Int) =
    for {
      tref       <- TRef.makeCommit(TreeSet.empty[Item])
      sequenceId <- TRef.makeCommit(0)
    } yield new Broadcast(tref, sequenceId, 100, mtu)

  final case class Item(seqId: Int, resend: Int, chunk: Chunk[Byte])

  object Item {
    implicit val ordering = Ordering.by[Item, Int](_.resend)
  }
}
