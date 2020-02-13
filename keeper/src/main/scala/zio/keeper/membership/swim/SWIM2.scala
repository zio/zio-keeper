package zio.keeper.membership.swim

import zio.keeper.discovery.Discovery
import zio.keeper.membership.swim.protocols.Initial
import zio.keeper.transport.Transport
import zio.keeper.{ Message, TaggedCodec }
import zio.stream.ZStream
import zio.{ Chunk, ZIO, ZManaged }

object SWIM2 {

  def run[A](local: A)(implicit initialCodec: TaggedCodec[Initial]) =
    for {
      env     <- ZManaged.environment[Transport[A] with Discovery[A]]
      nodes   <- Nodes.make[A](local).toManaged_
      initial <- Initial.protocol(nodes).toManaged_
      _ <- env.transport.bind(local) { conn =>
            val cc = new ClusterConnection(conn)
            ZStream
              .repeatEffect(cc.read)
              .mapM(
                msg => withProtocol(initial, conn.address, msg)
              )
              .runDrain
              .ignore
          }
    } yield ()

  def withProtocol[A, M: TaggedCodec](protocol: Protocol[A, M], addr: A, msg: Message) =
    TaggedCodec
      .read[M](msg.payload)
      .map(
        i =>
          protocol.onMessage
            .andThen(_.flatMap {
              case Some((addr, msg)) => TaggedCodec.write(msg).map(chunk => Some((addr, chunk)))
              case _                 => ZIO.succeed[Option[(A, Chunk[Byte])]](None)
            })
            .apply((addr, i))
      )
}
