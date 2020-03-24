package zio.keeper.membership.swim

import upickle.default._
import zio.keeper.membership.{ ByteCodec, NodeAddress }
import zio.{ Chunk, IO }

sealed trait Message[+A] {
  self =>
  val message: A

  final def transformM[B](fn: A => IO[zio.keeper.Error, B]): IO[zio.keeper.Error, Message[B]] =
    self match {
      case d: Message.Direct[A] =>
        fn(message).map(b => d.copy(message = b))
      case d: Message.Broadcast[A] =>
        fn(message).map(b => d.copy(message = b))
    }

  final def transform[B](fn: A => B): Message[B] =
    self match {
      case d: Message.Direct[A] =>
        d.copy(message = fn(message))
      case d: Message.Broadcast[A] =>
        d.copy(message = fn(message))
    }
}

object Message {

  case class Direct[A](node: NodeAddress, message: A) extends Message[A] {

    def reply[B <: A](message: B) =
      this.copy(message = message)
  }
  case class Broadcast[A](message: A) extends Message[A]
//  def unapply[A](arg: Message[A]): Option[(NodeId, A)] =
//    Some((arg.nodeId, arg.message))
//
//  def apply[A](nodeId: NodeId, message: A): Message[A] =
//    new Message(nodeId, message, UUID.randomUUID())
//
//  def apply[A](nodeId: NodeId, message: A, correlationId: UUID): Message[A] =
//    new Message(nodeId, message, correlationId)

  implicit val codec: ByteCodec[Message.Direct[Chunk[Byte]]] =
    ByteCodec.fromReadWriter(macroRW[Message.Direct[Chunk[Byte]]])

  implicit val chunkRW: ReadWriter[Chunk[Byte]] =
    implicitly[ReadWriter[Array[Byte]]]
      .bimap[Chunk[Byte]](
        ch => ch.toArray,
        arr => Chunk.fromArray(arr)
      )
}
