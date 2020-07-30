package zio.keeper.swim

import zio.keeper.swim.Broadcast._
import zio.stm.{ STM, TRef }
import zio.{ Chunk, UIO, URIO, ZIO }

import scala.collection.immutable.TreeSet

final class Broadcast(
  nodes: Nodes.Service,
  ref: TRef[TreeSet[Item]],
  sequenceId: TRef[Int],
  messageOverhead: Int,
  messageLimit: Int,
  resentMultiplier: Int
) {

  def add(message: Message.Broadcast[Chunk[Byte]]): UIO[Unit] =
    nodes.numberOfNodes
      .map(numberOfNodes => (math.log(numberOfNodes + 1.0) * resentMultiplier).toInt)
      .flatMap(
        resent =>
          sequenceId
            .getAndUpdate(_ + 1)
            .flatMap[Any, Nothing, Unit](
              seqId =>
                ref.update(
                  items => items ++ TreeSet(Item(seqId, resent, message.message))
                )
            )
            .commit
      )

  def broadcast(currentMessageSize: Int): UIO[List[Chunk[Byte]]] =
    ref.modify { items =>
      val (toSend, toReschedule, _) = items.foldRight((Vector.empty[Item], Vector.empty[Item], currentMessageSize)) {
        case (item, (toSend, toReschedule, size)) if size + item.chunk.size + messageOverhead <= messageLimit =>
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

  def make(mtu: Int, resentMultiplier: Int): URIO[Nodes, Broadcast] =
    ZIO.accessM[Nodes](
      nodes =>
        STM
          .mapN(TRef.make(TreeSet.empty[Item]), TRef.make(0))(new Broadcast(nodes.get, _, _, 3, mtu, resentMultiplier))
          .commit
    )

  final case class Item(seqId: Int, resend: Int, chunk: Chunk[Byte])
  implicit val ordering: Ordering[Item] = Ordering.by[Item, Int](_.seqId)

}
