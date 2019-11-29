package zio.keeper

import zio.Chunk

final case class Message(
  sender: NodeId,
  payload: Chunk[Byte]
)
