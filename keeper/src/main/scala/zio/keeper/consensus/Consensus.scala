package zio.keeper.consensus

import zio._
import zio.keeper.membership.NodeAddress
import zio.keeper.Error
import zio.stream.Stream

object Consensus {

  trait Service {
    def getLeader: UIO[NodeAddress]
    def onLeaderChange: Stream[Error, NodeAddress]
    def getView: UIO[Vector[NodeAddress]]
    def selfNode: UIO[NodeAddress]
    def receive: Stream[Error, (NodeAddress, Chunk[Byte])]
    def broadcast(data: Chunk[Byte]): IO[Error, Unit]
    def send(data: Chunk[Byte], to: NodeAddress): IO[Error, Unit]
  }

}
