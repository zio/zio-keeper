package zio.keeper.membership.swim

import zio.keeper.membership.swim.Broadcast._
import zio.stm.{ STM, TRef }
import zio.{ Chunk, UIO }

import scala.collection.immutable.TreeSet

final class Broadcast(
  ref: TRef[TreeSet[Item]],
  sequenceId: TRef[Int],
  messageOverhead: Int,
  messageLimit: Int,
  resent: Int
) {

  def add(message: Message.Broadcast[Chunk[Byte]]): UIO[Unit] =
    sequenceId
      .getAndUpdate(_ + 1)
      .flatMap[Any, Nothing, Unit](
        seqId =>
          ref.update(
            items =>
              items ++ TreeSet(Item(seqId, resent /* this should calculated based of num of nodes */, message.message))
          )
      )
      .commit

  def broadcast(currentMessageSize: Int): UIO[List[Chunk[Byte]]] =
    ref.modify { items =>
      val (toSend, toReschedule, _) = items.foldRight((Vector.empty[Item], Vector.empty[Item], currentMessageSize)) {
        case (item, (toSend, toReschedule, size))
            if item.chunk.size + messageOverhead <= messageLimit - currentMessageSize =>
          (toSend :+ item, toReschedule, size + item.chunk.size + messageOverhead)
        case (item, (toSend, toReschedule, size)) =>
          (toSend, toReschedule :+ item, size)
      }

      val newValue = TreeSet.empty[Item] ++ toSend
        .map(item => item.copy(resend = item.resend - 1))
        .filter(_.resend > 0) ++
        toReschedule

      val broadcast = toSend.map(item => item.chunk).toList
      (broadcast, newValue)
    }.commit

}

object Broadcast {

  def make(mtu: Int, resent: Int): UIO[Broadcast] =
    STM.mapN(TRef.make(TreeSet.empty[Item]), TRef.make(0))(new Broadcast(_, _, 100, mtu, resent)).commit

  final case class Item(seqId: Int, resend: Int, chunk: Chunk[Byte])
  implicit val ordering: Ordering[Item] = Ordering.by[Item, Int](_.resend)

}
