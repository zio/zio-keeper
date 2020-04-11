package zio.keeper.membership.swim

import upickle.default._
import zio.duration.Duration
import zio.keeper.membership.swim.Message.{NoResponse, WithTimeout}
import zio.keeper.membership.{ByteCodec, NodeAddress}
import zio.{Chunk, IO, ZIO, keeper}

sealed trait Message[+A] {
  self =>
  final def transformM[B](fn: A => IO[keeper.Error, B]): IO[keeper.Error, Message[B]] =
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
      case msg: WithTimeout[A] =>
        msg.message.transformM(fn).map(b => msg.copy(message = b, action = msg.action.flatMap(_.transformM(fn))))

      case NoResponse => ZIO.succeed(NoResponse)

    }

}

object Message {

  case class Direct[A](node: NodeAddress, message: A) extends Message[A]

  case class Batch[A](first: Message[A], second: Message[A], rest: Message[A]*) extends Message[A]
  case class Broadcast[A](message: A) extends Message[A]
  case class WithTimeout[A](message: Message[A], action: IO[keeper.Error, Message[A]], timeout: Duration) extends Message[A]
  case object NoResponse extends Message[Nothing]

  def withTimeout[R, A](message: Message[A], action: ZIO[R, keeper.Error, Message[A]], timeout: Duration) =
    for {
      env <- ZIO.environment[R]
    } yield WithTimeout(message, action.provide(env), timeout)

}
