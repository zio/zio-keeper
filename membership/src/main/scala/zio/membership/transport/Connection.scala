package zio.membership.transport

import zio.{ Chunk, ZIO }
import zio.stream.ZStream

trait Connection[R, E, A] {
  def send(data: Chunk[Byte]): ZIO[R, E, Unit]
  val receive: ZStream[R, E, A]
}
