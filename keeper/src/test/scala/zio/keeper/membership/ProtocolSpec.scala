package zio.keeper.membership

import zio.ZIO
import zio.keeper.{ NodeAddress, TaggedCodec }
import zio.keeper.membership.PingPong.{ Ping, Pong }
import zio.keeper.membership.swim.{ Message, Protocol }
import zio.stream.ZStream
import zio.test._
import zio.test.Assertion._
import zio.keeper.membership.swim.ConversationId

object ProtocolSpec extends DefaultRunnableSpec {

  val protocolDefinition = Protocol[PingPong].make(
    {
      case Message.Direct(sender, _, Ping(i)) =>
        Message.direct(sender, Pong(i))
      case _ => Message.noResponse
    },
    ZStream.empty
  )

  val testNode = NodeAddress(Array(1, 2, 3, 4), 123)

  val spec = suite("protocol spec")(
    testM("request response") {
      for {
        protocol <- protocolDefinition
        response <- protocol.onMessage(Message.Direct(testNode, 1, Ping(123)))
      } yield assert(response)(equalTo(Message.Direct(testNode, 1, Pong(123))))
    },
    testM("binary request response") {
      for {
        protocol       <- protocolDefinition.map(_.binary)
        binaryMessage  <- TaggedCodec.write[PingPong](Ping(123))
        responseBinary <- protocol.onMessage(Message.Direct(testNode, 1, binaryMessage))
        response <- responseBinary match {
                     case Message.Direct(addr, conversationId, chunk) =>
                       TaggedCodec.read[PingPong](chunk).map(pp => Message.Direct(addr, conversationId, pp))
                     case _ => ZIO.succeed(Message.NoResponse)
                   }
      } yield assert(response)(equalTo(Message.Direct[PingPong](testNode, 1, Pong(123))))
    }
  ).provideLayer(ConversationId.live)

}
