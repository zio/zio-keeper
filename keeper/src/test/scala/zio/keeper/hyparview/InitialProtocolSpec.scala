package zio.keeper.hyparview

import zio.test._
import zio.test.Assertion._
import zio.keeper.KeeperSpec
import zio.keeper.gens
import zio.keeper.transport.testing.MockConnection
import zio.logging.Logging

object InitialProtocolSpec extends KeeperSpec {

  def spec =
    suite("InitialProtocol")(
      suite("on receiving a neighbor message")(
        testM("should reject if not high priority and activeView is full") {
          checkM(gens.nodeAddress, gens.nodeAddress) { case (localAddress, remoteAddress) =>
            val makeConnection = {
              import MockConnection._
              import MockConnection.Script._
              make(
                emit(Message.Neighbor(remoteAddress, false)) ++ await[Message](equalTo(Message.NeighborReject))
              )
            }
            val test = makeConnection.use { con =>
              for {
                protoResult <- protocols.initialProtocol.run(con)
                viewsResult <- Views.passiveView.map(_.contains(remoteAddress)).commit
              } yield protoResult.map((_, viewsResult))
            }
            val env = defaultEnv >+> Views.live(localAddress, 0, 1)
            assertM(test.provideLayer(env))(isSome(equalTo((None, true))))
          }
        },
        testM("should accept if high priority and activeView is full") {
          checkM(gens.nodeAddress, gens.nodeAddress) { case (localAddress, remoteAddress) =>
            val makeConnection = {
              import MockConnection._
              import MockConnection.Script._
              make(
                emit(Message.Neighbor(remoteAddress, true)) ++ await[Message](equalTo(Message.NeighborAccept))
              )
            }
            val test = makeConnection.use { con =>
              for {
                protoResult <- protocols.initialProtocol.run(con)
                viewsResult <- Views.passiveView.map(_.contains(remoteAddress)).commit
              } yield protoResult.map((_, viewsResult))
            }
            val env = defaultEnv >+> Views.live(localAddress, 0, 1)
            assertM(test.provideLayer(env))(isSome(equalTo((Some(remoteAddress), false))))
          }
        }
      )
    )

  private val defaultEnv =
    TRandom.live >+>
    {
      Logging.ignore ++
      Views.live(address(0), 10, 10) ++
      HyParViewConfig.staticConfig(10, 10, 5, 3, 2, 2, 3, 256, 256, 256)
    }

}
