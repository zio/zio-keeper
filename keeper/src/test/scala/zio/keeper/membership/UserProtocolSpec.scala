package zio.keeper.membership

import upickle.default.macroRW
import zio.keeper.membership.swim.protocols.User
import zio.test.Assertion._
import zio.test._

object UserProtocolSpec extends DefaultRunnableSpec {

  val spec = suite("User Protocol Serialization")(
    testM("Ping read and write") {
      val ping0: User[PingPong] = User(PingPong.Ping(1))
      val pong0: User[PingPong] = User(PingPong.Pong(1))
      for {
        pingChunk <- TaggedCodec.write[User[PingPong]](ping0)
        ping      <- TaggedCodec.read[User[PingPong]](pingChunk)
        pongChunk <- TaggedCodec.write[User[PingPong]](pong0)
        pong      <- TaggedCodec.read[User[PingPong]](pongChunk)
      } yield assert(ping)(equalTo(ping0)) && assert(pong)(equalTo(pong0))
    }
  )

}
