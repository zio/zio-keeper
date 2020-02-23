package zio.keeper.membership.swim

import java.util.UUID

import zio.Chunk

case class Message(
                    nodeId: NodeId,
                    payload: Chunk[Byte],
                    correlationId: UUID
                  )
