package zio.keeper.membership.swim

import zio.ZIO
import zio.keeper.ClusterError.UnexpectedMessage
import zio.keeper.membership.swim.Protocol.Reply
import zio.keeper.{Error, Message, TaggedCodec}

trait Protocol[R, M] {
  def run(m: Message): ZIO[R, Error, Reply[M]]
}

object Protocol {

  sealed trait Reply[M]
  case object Empty extends Reply[Nothing]
  case class MessageReply[M](m: M) extends Reply[M]

  def noReply[M]: Reply[M] = Empty.asInstanceOf[Reply[M]]
  def reply[M: TaggedCodec](m: M): ZIO[Any, Error, Reply[M]] = ???

  def apply[R, M: TaggedCodec](pf: PartialFunction[M, ZIO[R, Error, Reply[M]]]): Protocol[R, M] =
    new Protocol[R, M] {
      override def run(m: Message): ZIO[R, Error, Reply[M]] = for {
        msg <- TaggedCodec.read(m.payload)
        reply <- pf.lift(msg).getOrElse(ZIO.fail(UnexpectedMessage(m)))
      } yield reply
    }

}
