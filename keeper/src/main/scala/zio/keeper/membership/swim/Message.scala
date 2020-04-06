package zio.keeper.membership.swim

import upickle.default._
import zio.keeper.membership.swim.Message.NoResponse
import zio.keeper.membership.{ByteCodec, NodeAddress}
import zio.{Chunk, IO, ZIO}

sealed trait Message[+A] {
  self =>
  final def transformM[B](fn: A => IO[zio.keeper.Error, B]): IO[zio.keeper.Error, Message[B]] =
    self match {
      case msg: Message.Direct[A] =>
        fn(msg.message).map(b => msg.copy(message = b))
      case msg: Message.Broadcast[A] =>
        fn(msg.message).map(b => msg.copy(message = b))
      case msg: Message.Batch[A] =>
        for {
          m1 <- msg.first.transformM(fn)
          m2 <- msg.second.transformM(fn)
          rest <- ZIO.foreach(msg.rest)(_.transformM(fn))
        } yield Message.Batch(m1, m2, rest :_*)
      case NoResponse => ZIO.succeed(NoResponse)

    }

}

object Message {

  case class Direct[A](node: NodeAddress, message: A) extends Message[A]
  case class WithPiggyback(node: NodeAddress, message: Chunk[Byte], gossip: List[Chunk[Byte]]) extends Message[Chunk[Byte]]
  case class Batch[A](first: Message[A], second: Message[A], rest: Message[A]*) extends Message[A]
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
