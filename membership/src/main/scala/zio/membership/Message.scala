package zio.membership

import zio.Chunk

final case class Message(
  msgType: MessageType,
  payload: Chunk[Byte]
)
