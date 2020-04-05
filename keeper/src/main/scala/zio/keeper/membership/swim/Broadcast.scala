package zio.keeper.membership.swim

import zio.{Chunk, ZIO}

class Broadcast {


  //this should use some sort of priority queue

  def add(message: Message.Broadcast[Chunk[Byte]]) = ???
  def broadcast(currentMessageSize: Int): ZIO[Any, Nothing, List[Chunk[Byte]]] = ZIO.succeed(List.empty)

}
