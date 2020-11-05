package zio.keeper.hyparview

import zio.keeper.KeeperSpec
import zio.test._
import zio.keeper.transport.testing.MockConnection._
import zio.keeper.NodeAddress
import zio._
import zio.keeper.gens
import zio.test.Assertion._
import zio.logging.Logging
import zio.keeper.transport.testing.MockConnection

object ActiveProtocolSpec extends KeeperSpec {

  def spec =
    suite("ActiveProtocol")(
      suite("on receiving disconnect")(
        testM("should keep remote in passive view if alive") {
          checkM(gens.nodeAddress) { address =>
            val test =
              for {
                result <- run(
                           address,
                           emit(Message.Disconnect(true))
                         )(isEmpty)
                passive <- Views.passiveView.commit
              } yield result && assert(passive)(contains(address))
            test.provideSomeLayer(defaultEnv)
          }
        },
        testM("should not keep remote in passive view if not alive") {
          checkM(gens.nodeAddress) { address =>
            val test = for {
              result <- run(
                         address,
                         emit(Message.Disconnect(false))
                       )(isEmpty)
              passive <- Views.passiveView.commit
            } yield result && assert(passive)(isEmpty)
            test.provideSomeLayer(defaultEnv)
          }
        }
      )
    )

  private def run(sender: NodeAddress, script: MockConnection[Nothing, Message, Message])(
    assertion: Assertion[List[(NodeAddress, Message.PeerMessage)]]
  ) = {
    val makeConnection = emit(Message.Join(sender)) ++ await[Message](equalTo(Message.JoinReply(address(0)))) ++ script
    makeConnection.use { con =>
      for {
        peerMessages <- Queue.unbounded[(NodeAddress, Message.PeerMessage)]
        result <- protocols
                   .hyparview(con, peerMessages)
                   .run
        out <- peerMessages.takeAll
      } yield assert(result)(succeeds(anything)) && assert(out)(assertion)
    }
  }

  private val defaultEnv =
    ZLayer.identity[Sized] ++
      TRandom.live ++
      Logging.ignore ++
      HyParViewConfig.static(address(0), 10, 10, 5, 3, 2, 2, 3, 256, 256, 256) >+>
      Views.live

}
