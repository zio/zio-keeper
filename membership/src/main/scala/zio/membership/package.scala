package zio

import zio.stream.ZStream
import zio.duration.Duration

package object membership extends Membership.Service[Membership] {

  def nodes =
    ZIO.accessM(_.membership.nodes)

  def receive =
    ZStream.unwrap {
      ZIO.environment.map(_.membership.receive)
    }

  def registerBroadcast(period: Duration, retransmits: Int) =
    ZManaged.environment[Membership].flatMap(_.membership.registerBroadcast(period, retransmits))

  def sendMessage(to: NodeId, data: Chunk[Byte]) =
    ZIO.accessM(_.membership.sendMessage(to, data))

  def sendMessageReply(to: NodeId, data: Chunk[Byte], timeout: Duration) =
    ZIO.accessM(_.membership.sendMessageReply(to, data, timeout))
}
