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
   * Our low level transport interface that allows sending messages either in fire-and-forget
   * or in streaming style. Also allows listening to messages sends from other nodes.
   *
   * The default implementation of this should use both TCP and UDP and bind both protocols
   * to the same port number on bind.
   */
  trait Service[R] {
    def sendBestEffort(to: SocketAddress, msg: Chunk[Byte]): ZIO[R, Error, Unit]
    def sendReliable(to: SocketAddress, msg: Chunk[Byte]): ZIO[R, Error, Unit]
    def bind(addr: SocketAddress): ZStream[R, Error, Chunk[Byte]]
  }
}
