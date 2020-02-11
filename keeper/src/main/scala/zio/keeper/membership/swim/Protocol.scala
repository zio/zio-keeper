package zio.keeper.membership.swim

import zio.ZIO
import zio.duration.Duration
import zio.keeper.membership.NodeId
import zio.keeper.membership.swim.Protocol.{Empty, Reply}
import zio.keeper.{Error, TaggedCodec}

trait Protocol[M] {

  def onMessage: PartialFunction[(NodeId, M), ZIO[Any, Error, Reply[M]]]

  def produceMessages: zio.stream.ZStream[Any, Error, (NodeId, M)]

  //def run to sa tylko response. musimy dowac co protokul robi, czy periodically czy nie
  //  def run(m: Message): ZIO[R, Error, Reply[M]]

  protected def noReply[M]: Reply[M] = Empty.asInstanceOf[Reply[M]]

  protected def reply[M: TaggedCodec](m: M): ZIO[Any, Error, Reply[M]] = ???

  protected def forward[M: TaggedCodec](nodeId: NodeId, m: M): ZIO[Any, Error, Reply[M]] = ???

  protected def withAck[M: TaggedCodec](ackId: Long, r: Reply[M], duration: Duration): ZIO[Any, Error, Reply[M]] = ???

  def onError(err: Error): ZIO[Any, Error, Unit]
}

object Protocol {

  sealed trait Reply[M]
  case object Empty extends Reply[Nothing]
  case class MessageReply[M](m: M) extends Reply[M]
  case class MessageForward[M](m: M) extends Reply[M]
  case class WithAck[M](ackId: Long, r: Reply[M], timeout: Duration) extends Reply[M]




//  def apply[R, M: TaggedCodec](pf: PartialFunction[M, ZIO[R, Error, Reply[M]]]): Protocol[R, M] =
//    new Protocol[R, M] {
//      override def run(m: Message): ZIO[R, Error, Reply[M]] = for {
//        msg <- TaggedCodec.read(m.payload)
//        reply <- pf.lift(msg).getOrElse(ZIO.fail(UnexpectedMessage(m)))
//      } yield reply
//    }

}
