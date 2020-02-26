package zio.keeper.membership

import upickle.default.macroRW
import zio.keeper.membership.swim.protocols.User
import zio.keeper.{ ByteCodec, TaggedCodec }
import zio.test.Assertion._
import zio.test._

object UserProtocolSpec
    extends DefaultRunnableSpec({
      suite("User Protocol Serialization")(
        testM("Ping read and write") {
          val ping0: User[PingPong] = User(PingPong.Ping(1))
          val pong0: User[PingPong] = User(PingPong.Pong(1))
          for {
            pingChunk <- TaggedCodec.write[User[PingPong]](ping0)
            ping      <- TaggedCodec.read[User[PingPong]](pingChunk)
            pongChunk <- TaggedCodec.write[User[PingPong]](pong0)
            pong      <- TaggedCodec.read[User[PingPong]](pongChunk)
          } yield assert(ping, equalTo(ping0)) && assert(pong, equalTo(pong0))
        }
      )

    })
sealed trait PingPong

object PingPong {
  case class Ping(i: Int) extends PingPong
  case class Pong(i: Int) extends PingPong

  implicit val pingCodec: ByteCodec[Ping] =
    ByteCodec.fromReadWriter(macroRW[Ping])

  implicit val pongCodec: ByteCodec[Pong] =
    ByteCodec.fromReadWriter(macroRW[Pong])

  implicit def tagged(implicit p1: ByteCodec[Ping], p2: ByteCodec[Pong]): TaggedCodec[PingPong] =
    TaggedCodec.instance[PingPong]({
      case Ping(_) => 1
      case Pong(_) => 2
    }, {
      case 1 => p1.asInstanceOf[ByteCodec[PingPong]]
      case 2 => p2.asInstanceOf[ByteCodec[PingPong]]
    })
}
