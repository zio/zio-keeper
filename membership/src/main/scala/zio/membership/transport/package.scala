package zio.membership

import zio._
import zio.nio.SocketAddress
import zio.stream._

package object transport extends Transport.Service[Transport] {

  override def bind(addr: SocketAddress) =
    ZStream.unwrap {
      ZIO.environment.map(_.transport.bind(addr))
    }

  override def send(to: SocketAddress, data: Chunk[Byte]) =
    ZIO.accessM(_.transport.send(to, data))

}
