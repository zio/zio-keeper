package zio.membership.hyparview

import upickle.default._
import zio.membership.ByteCodec
import zio._

sealed trait Protocol[T]

object Protocol {
  def decodeChunk[T](chunk: Chunk[Byte]): UIO[Protocol[T]] = ???

  final case class Neighbor[T](
    sender: T,
    isHighPriority: Boolean
  ) extends Protocol[T]

  final case class NeighborReject[T](
    sender: T
  ) extends Protocol[T]

  final case class Disconnect[T](
    sender: T,
    alive: Boolean
  ) extends Protocol[T]

  object Disconnect {
    implicit def rw[T: ReadWriter]: ReadWriter[Disconnect[T]]                               = macroRW[Disconnect[T]]
    implicit def codec[T](implicit ev: ReadWriter[Disconnect[T]]): ByteCodec[Disconnect[T]] = ByteCodec.fromReadWriter
  }

  final case class Join[T](
    sender: T
  ) extends Protocol[T]

  final case class ForwardJoin[T](
    sender: T,
    originalSender: T,
    ttl: TimeToLive
  ) extends Protocol[T]

  final case class Shuffle[T](
    sender: T,
    originalSender: T,
    activeNodes: List[T],
    passiveNodes: List[T],
    ttl: TimeToLive
  ) extends Protocol[T]

  final case class ShuffleReply[T](
    passiveNodes: List[T],
    sentOriginally: List[T]
  ) extends Protocol[T]
}
