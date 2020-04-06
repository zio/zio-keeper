package zio.membership.transport

import zio._
import zio.stream.ZStream

trait Connection[R, E, A] {
  def send(data: A): ZIO[R, E, Unit]
  val receive: ZStream[R, E, A]
  val close: UIO[Unit]
}
