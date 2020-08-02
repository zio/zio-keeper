package zio.keeper.transport

import zio._
import zio.stream.ZStream

trait Connection[-R, +E, -I, +O] {
  def send(data: I): ZIO[R, E, Unit]
  val receive: ZStream[R, E, O]
  val close: UIO[Unit]
}
