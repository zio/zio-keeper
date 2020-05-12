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
      msg => userIn.offer(Message.Direct(msg.node, msg.message.msg)).as(Message.NoResponse),
      ZStream
        .fromQueue(userOut)
        .collect {
          case Message.Direct(node, msg) => Message.Direct(node, User(msg))
        }
    )

}
