package zio.keeper

import upickle.default._
import zio.keeper.TransportError._
import zio.nio.core.{ InetAddress, InetSocketAddress, SocketAddress }
import zio.{ Chunk, IO, UIO }

final case class NodeAddress(ip: Chunk[Byte], port: Int) {

  override def equals(obj: Any): Boolean = obj match {
    case NodeAddress(ip, port) => this.port == port && ip.sameElements(this.ip)
    case _                     => false
  }

  override def hashCode(): Int = port

  def socketAddress: IO[TransportError, InetSocketAddress] =
    (for {
      addr <- InetAddress.byAddress(ip.toArray)
      sa   <- SocketAddress.inetSocketAddress(addr, port)
    } yield sa).mapError(ExceptionWrapper)

  override def toString: String = ip.mkString(".") + ": " + port
}

object NodeAddress {

  def fromSocketAddress(addr: InetSocketAddress): UIO[NodeAddress] =
    InetAddress
      .byName(addr.hostString)
      .map(inet => NodeAddress(Chunk.fromArray(inet.address), addr.port))
      .orDie

  def local(port: Int): UIO[NodeAddress] =
    InetAddress.localHost
      .map(addr => NodeAddress(Chunk.fromArray(addr.address), port))
      .orDie

  implicit val nodeAddressRw: ReadWriter[NodeAddress] = macroRW[(Array[Byte], Int)].bimap(n => (n.ip.toArray, n.port), {
    case (ip, port) => NodeAddress(Chunk.fromArray(ip), port)
  })

  implicit val byteCodec: ByteCodec[NodeAddress] =
    ByteCodec.fromReadWriter(nodeAddressRw)

}
