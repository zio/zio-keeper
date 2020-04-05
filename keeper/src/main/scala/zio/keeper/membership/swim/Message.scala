package zio.keeper.membership.swim

import upickle.default._
import zio.keeper.membership.{ ByteCodec, NodeAddress }
import zio.{ Chunk, IO }

sealed trait Message[+A] {
  self =>
//  val message: A
//
//  final def transformM[B](fn: A => IO[zio.keeper.Error, B]): IO[zio.keeper.Error, Message[B]] =
//    self match {
//      case d: Message.Direct[A] =>
//        fn(message).map(b => d.copy(message = b))
//      case d: Message.Broadcast[A] =>
//        fn(message).map(b => d.copy(message = b))
//    }
//
//  final def transform[B](fn: A => B): Message[B] =
//    self match {
//      case d: Message.Direct[A] =>
//        d.copy(message = fn(message))
//      case d: Message.Broadcast[A] =>
//        d.copy(message = fn(message))
//    }
}

object Message {

  case class Direct[A](node: NodeAddress, message: A) extends Message[A]
  case class WithPiggyback(node: NodeAddress, message: Chunk[Byte], gossip: List[Chunk[Byte]]) extends Message[Chunk[Byte]]
  case class Broadcast[A](message: A) extends Message[A]
  case object NoResponse extends Message[Nothing]

  implicit val codec: ByteCodec[Message.WithPiggyback] =
    ByteCodec.fromReadWriter(macroRW[Message.WithPiggyback])

  implicit val chunkRW: ReadWriter[Chunk[Byte]] =
    implicitly[ReadWriter[Array[Byte]]]
      .bimap[Chunk[Byte]](
        ch => ch.toArray,
        arr => Chunk.fromArray(arr)
      )
}
