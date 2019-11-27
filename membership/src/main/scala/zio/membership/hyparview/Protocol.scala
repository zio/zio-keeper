package zio.membership.hyparview
import zio.membership.ByteCodec
import zio._

sealed trait Protocol[T]

object Protocol {
  def decodeChunk[T](chunk: Chunk[Byte]): UIO[Protocol[T]] = ???

  final case class Neighbor[T](
    sender: T,
    isHighPriority: Boolean
  ) extends Protocol[T]

  final case class Disconnect[T](
    sender: T,
    alive: Boolean
  ) extends Protocol[T]

  final case class Join[T](
    sender: T
  ) extends Protocol[T]

  final case class ForwardJoin[T](
    sender: T,
    originalSender: T,
    ttl: TimeToLive
  ) extends Protocol[T]

  object ForwardJoin {
    implicit def codec[T]: ByteCodec[Any, ForwardJoin[T]] = ???
  }

  final case class Shuffle[T](
    sender: T,
    activeNodes: List[T],
    passiveNodes: List[T],
    ttl: TimeToLive
  ) extends Protocol[T]

  final case class ShuffleReply[T](
    sender: T,
    passiveNodes: List[T]
  ) extends Protocol[T]
}
