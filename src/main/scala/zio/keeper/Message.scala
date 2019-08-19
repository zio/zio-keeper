package zio.keeper

import zio.Chunk
import zio.nio.channels.AsynchronousSocketChannel

final case class Message(sender: NodeId, payload: Chunk[Byte], replyTo: AsynchronousSocketChannel)
