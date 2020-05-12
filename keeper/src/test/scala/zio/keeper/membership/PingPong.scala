package zio.keeper.membership

import upickle.default.macroRW
import zio.keeper.ByteCodec

sealed trait PingPong

object PingPong {
  final case class Ping(i: Int) extends PingPong

  object Ping {

    implicit val pingCodec: ByteCodec[Ping] =
      ByteCodec.fromReadWriter(macroRW[Ping])
  }

  final case class Pong(i: Int) extends PingPong

  object Pong {

    implicit val pongCodec: ByteCodec[Pong] =
      ByteCodec.fromReadWriter(macroRW[Pong])
  }

  implicit val codec: ByteCodec[PingPong] =
    ByteCodec.tagged[PingPong][
      Ping,
      Pong
    ]
}
