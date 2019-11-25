package zio

import zio.stream.ZStream

package object membership extends Membership.Service[Membership] {

  override val nodes =
    ZIO.accessM(_.membership.nodes)

  override def send[R1 <: Membership, A](to: Member, data: A)(implicit ev: ByteCodec[R1, A]) =
    ZIO.accessM(_.membership.send(to, data))

  override def broadcast[R1 <: Membership, A](data: A)(implicit ev: ByteCodec[R1, A]) =
    ZIO.accessM(_.membership.broadcast(data))

  override def receive =
    ZStream.unwrap {
      ZIO.environment.map(_.membership.receive)
    }

}
