package zio.keeper.membership.swim

import zio._
import zio.keeper.{ ByteCodec, KeeperSpec, NodeAddress }
import zio.keeper.membership.swim.Messages.WithPiggyback
import zio.keeper.membership.swim.PingPong.{ Ping, Pong }
import zio.logging.Logging
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._

object MessagesSpec extends KeeperSpec {

  val logger = Logging.console((_, line) => line)

  val messages = for {
    local     <- NodeAddress.local(1111).toManaged_
    transport <- TestTransport.make
    broadcast <- Broadcast.make(64000, 2).toManaged_
    messages  <- Messages.make(local, broadcast, transport)
  } yield (transport, messages)

  val spec = suite("messages")(
    testM("receiveMessage") {
      val testNodeAddress = NodeAddress(Array(1, 2, 3, 4), 1111)

      val protocol = Protocol[PingPong].make(
        {
          case Message.Direct(sender, _, Ping(i)) =>
            Message.direct(sender, Pong(i))
          case _ =>
            Message.noResponse
        },
        ZStream.empty
      )

      messages.use {
        case (testTransport, messages) =>
          for {
            dl    <- protocol
            _     <- messages.process(dl.binary)
            ping1 <- ByteCodec[PingPong].toChunk(PingPong.Ping(123))
            ping2 <- ByteCodec[PingPong].toChunk(PingPong.Ping(321))
            _     <- testTransport.incommingMessage(WithPiggyback(testNodeAddress, 1, ping1, List.empty))
            _     <- testTransport.incommingMessage(WithPiggyback(testNodeAddress, 2, ping2, List.empty))
            messages <- testTransport.outgoingMessages
                         .mapM {
                           case WithPiggyback(_, _, chunk, _) =>
                             ByteCodec.decode[PingPong](chunk)
                         }
                         .take(2)
                         .runCollect
          } yield assert(messages)(hasSameElements(List(PingPong.Pong(123), PingPong.Pong(321))))
      }
    },
    testM("should not exceed size of message") {
      val testNodeAddress = NodeAddress(Array(1, 2, 3, 4), 1111)

      val protocol = Protocol[PingPong].make(
        {
          case Message.Direct(sender, _, Ping(i)) =>
            ZIO.succeed(
              Message.Batch(
                Message.Direct(sender, 1, Pong(i)),
                Message.Broadcast(Pong(i)),
                List.fill(2000)(Message.Broadcast(Ping(1))): _*
              )
            )
          case _ =>
            Message.noResponse
        },
        ZStream.empty
      )

      messages.use {
        case (testTransport, messages) =>
          for {
            dl    <- protocol
            _     <- messages.process(dl.binary)
            ping1 <- ByteCodec[PingPong].toChunk(PingPong.Ping(123))
            _     <- testTransport.incommingMessage(WithPiggyback(testNodeAddress, 1, ping1, List.empty))
            m :: Nil <- testTransport.outgoingMessages
                         .take(1)
                         .runCollect
            bytes <- ByteCodec[WithPiggyback].toChunk(m)
          } yield assert(m.gossip.size)(equalTo(1141)) && assert(bytes.size)(equalTo(62868))
      }
    }
  ).provideCustomLayer(logger ++ ConversationId.live)
}
