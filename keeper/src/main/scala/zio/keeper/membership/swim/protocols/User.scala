package zio.keeper.membership.swim.protocols

import zio.keeper.ByteCodec
import zio.keeper.membership.swim.{ Message, Protocol }
import zio.stream._

final case class User[A](msg: A) extends AnyVal

object User {

  implicit def codec[A](implicit ev: ByteCodec[A]): ByteCodec[User[A]] =
    ev.bimap(User.apply, _.msg)

  def protocol[B: ByteCodec](
    userIn: zio.Queue[Message.Direct[B]],
    userOut: zio.Queue[Message.Direct[B]]
  ) =
    Protocol[User[B]].make(
      msg => Message.direct(msg.node, msg.message.msg).flatMap(userIn.offer(_)).as(Message.NoResponse),
      ZStream
        .fromQueue(userOut)
        .collect {
          case Message.Direct(node, conversationId, msg) =>
            Message.Direct(node, conversationId, User(msg))
        }
    )

}
