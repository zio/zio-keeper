package zio.keeper.hyparview

import zio.keeper.KeeperSpec
import zio.test._
import zio.keeper.transport.testing.{ MockConnection => MC }
import zio.keeper.NodeAddress
import zio._
import zio.keeper.gens
import zio.test.Assertion._
import zio.logging.Logging

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
                           MC.emit(Message.Disconnect(true))
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
                         MC.emit(Message.Disconnect(false))
                       )(isEmpty)
              passive <- Views.passiveView.commit
            } yield result && assert(passive)(isEmpty)
            test.provideSomeLayer(defaultEnv)
          }
        }
      )
    )

  private def run(sender: NodeAddress, script: MC.Script[TestResult, Message, Message])(
    assertion: Assertion[List[(NodeAddress, Message.PeerMessage)]]
  ) =
    MC.make(MC.emit(Message.Join(sender)) ++ MC.await[Message](equalTo(Message.JoinReply(address(0)))) ++ script).use {
      con =>
        for {
          peerMessages <- Queue.unbounded[(NodeAddress, Message.PeerMessage)]
          result <- protocols
                     .hyparview(con, peerMessages)
                     .foldM(ZIO.succeedNow, _ => peerMessages.takeAll.map(assert(_)(assertion)))
        } yield result
    }

  private val defaultEnv =
    ZLayer.identity[Sized] ++
      TRandom.live ++
      Logging.ignore ++
      HyParViewConfig.static(address(0), 10, 10, 5, 3, 2, 2, 3, 256, 256, 256) >+>
      Views.live

}
