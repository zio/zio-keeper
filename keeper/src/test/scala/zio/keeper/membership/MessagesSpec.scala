package zio.keeper.membership

import zio._
import zio.keeper.{ ByteCodec, NodeAddress, TransportError }
import zio.keeper.membership.PingPong.{ Ping, Pong }
import zio.keeper.membership.swim.Messages.WithPiggyback
import zio.keeper.membership.swim.{ Broadcast, Message, Messages, Protocol }
import zio.keeper.transport.{ Bind, Channel, ConnectionLessTransport }
import zio.logging.Logging
import zio.nio.core.SocketAddress
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._
import zio.keeper.membership.swim.ConversationId

object MessagesSpec extends DefaultRunnableSpec {

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
            m <- testTransport.outgoingMessages
                  .mapM { case WithPiggyback(_, _, chunk, _) => 
                    ByteCodec.decode[PingPong](chunk)
                  }
                  .take(2)
                  .runCollect
          } yield assert(m)(hasSameElements(List(PingPong.Pong(123), PingPong.Pong(321))))
      }
    }
  ).provideCustomLayer(logger ++ ConversationId.live)

}
