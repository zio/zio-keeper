package zio.keeper.membership.swim

import zio.stm.TMap
import zio.{Chunk, ZIO}

class Broadcast {

  //this should use some sort of priority queue

  def add(message: Message.Broadcast[Chunk[Byte]]): ZIO[Any, Nothing, Unit] = ???
  def broadcast(currentMessageSize: Int): ZIO[Any, Nothing, List[Chunk[Byte]]] = ZIO.succeed(List.empty)

}
