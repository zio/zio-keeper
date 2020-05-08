package zio.keeper.membership

import zio.{ Chunk, ZIO }
import zio.keeper.membership.swim.{ Broadcast, Message }
import zio.test._
import zio.test.Assertion._

object BroadcastSpec extends DefaultRunnableSpec {

  val spec = suite("broadcast")(
    testM("add and retrieve from broadcast") {
      for {
        broadcast <- Broadcast.make(500, 2)
        _         <- broadcast.add(Message.Broadcast(Chunk.fromArray(Array.fill[Byte](100)(1))))
        _         <- broadcast.add(Message.Broadcast(Chunk.fromArray(Array.fill[Byte](50)(2))))
        _         <- broadcast.add(Message.Broadcast(Chunk.fromArray(Array.fill[Byte](200)(3))))
        result    <- broadcast.broadcast(100)
      } yield assert(result)(equalTo(List(Chunk.fromArray(Array.fill[Byte](200)(3)))))

    },
    testM("resent message") {
      for {
        broadcast <- Broadcast.make(500, 2)
        _         <- broadcast.add(Message.Broadcast(Chunk.fromArray(Array.fill[Byte](100)(1))))
        result <- ZIO.reduceAll(
                   ZIO.succeedNow(List.empty[Chunk[Byte]]),
                   (1 to 3).map(_ => broadcast.broadcast(100))
                 )(_ ++ _)
      } yield assert(result.size)(equalTo(2))

    }
  )
}
