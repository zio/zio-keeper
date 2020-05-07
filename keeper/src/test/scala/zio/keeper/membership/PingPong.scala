package zio.keeper.membership

import upickle.default.macroRW
import zio.keeper.{ ByteCodec, TaggedCodec }

sealed trait PingPong

object PingPong {
  case class Ping(i: Int) extends PingPong
  case class Pong(i: Int) extends PingPong

  implicit val pingCodec: ByteCodec[Ping] =
    ByteCodec.fromReadWriter(macroRW[Ping])

  implicit val pongCodec: ByteCodec[Pong] =
    ByteCodec.fromReadWriter(macroRW[Pong])

  implicit def tagged(implicit p1: ByteCodec[Ping], p2: ByteCodec[Pong]) =
    TaggedCodec.instance[PingPong]({
      case Ping(_) => 1
      case Pong(_) => 2
    }, {
      case 1 => p1.asInstanceOf[ByteCodec[PingPong]]
      case 2 => p2.asInstanceOf[ByteCodec[PingPong]]
    })

}
