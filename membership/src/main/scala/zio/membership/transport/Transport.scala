package zio.membership.transport

import zio._
import zio.stream._
import zio.membership.TransportError

trait Transport[T] {
  val transport: Transport.Service[Any, T]
}

object Transport {

  /**
   * Our low level transport interface that allows sending messages.
   * Also allows listening to messages sends from other nodes.
   */
  trait Service[R, T] {
    def send(to: T, data: Chunk[Byte]): ZIO[R, TransportError, Unit]
    def bind(addr: T): ZStream[R, TransportError, Chunk[Byte]]
  }
}
