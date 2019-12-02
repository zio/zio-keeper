package zio.membership.transport

import zio._
import zio.stream._

trait Connection[R, E, A] {

  def send(data: Chunk[Byte]): ZIO[R, E, Unit]
  val receive: ZStream[R, E, A]
  val close: UIO[Unit]

}
