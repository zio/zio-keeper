package zio.keeper

import zio.Chunk
import zio.Task

final case class Message(sender: NodeId, payload: Chunk[Byte], reply: Chunk[Byte] => Task[Unit])
