package zio.membership.transport

import zio._
import zio.stream._
import zio.nio.SocketAddress

trait Transport {
  val transport: Transport.Service[Any]
}

object Transport {

  /**
   * TODO: Naming should be discussed.
   * Our low level transport interface that allows sending messages.
   * Also allows listening to messages sends from other nodes.
   */
  trait Service[R] {
    def send(to: SocketAddress, data: Chunk[Byte]): ZIO[R, Error, Unit]
    def bind(addr: SocketAddress): ZStream[R, Error, Chunk[Byte]]
  }
}
