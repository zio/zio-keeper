package zio.membership

import zio._
import zio.stream._
import zio.nio.SocketAddress

package object transport extends Transport.Service[Transport] {

  def sendBestEffort(to: SocketAddress, msg: Chunk[Byte]) =
    ZIO.accessM(_.transport.sendBestEffort(to, msg))

  def sendReliable(to: SocketAddress, msg: Chunk[Byte]) =
    ZIO.accessM(_.transport.sendReliable(to, msg))

  def bind(addr: SocketAddress) =
    ZStream.unwrap {
      ZIO.environment.map(_.transport.bind(addr))
    }

}
