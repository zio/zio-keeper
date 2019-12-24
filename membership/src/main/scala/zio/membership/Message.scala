package zio.membership

import zio.Chunk

final case class Message(
  from: Sender,
  msgType: MessageType,
  payload: Chunk[Byte]
)
