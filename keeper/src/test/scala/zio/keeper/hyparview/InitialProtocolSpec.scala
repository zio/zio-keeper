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
      suite("on receiving Join")(
        testM("should succeed with sender of messagee") {
          checkM(gens.nodeAddress, gens.nodeAddress) {
            case (localAddress, remoteAddress) =>
              val makeConnection = {
                import MockConnection._
                make(
                  emit(Message.Join(remoteAddress))
                    ++ await[Message](equalTo(Message.JoinReply(localAddress)))
                )
              }
              val test = makeConnection.use { con =>
                for {
                  protoResult <- protocols.initialProtocol.run(con)
                } yield protoResult.flatten
              }
              val env = defaultEnv >+> Views.live(localAddress, 1, 1)
              assertM(test.provideLayer(env))(isSome(equalTo(remoteAddress)))
          }
        },
        test("should send ForwardJoin to every other member of active view") {
          assert(())(anything) // TODO
        }
      ),
      testM("succeeds with sender on receiving ForwardJoinReply") {
        checkM(gens.nodeAddress) { remoteAddress =>
          val makeConnection = {
            import MockConnection._
            make(
              emit(Message.ForwardJoinReply(remoteAddress))
            )
          }
          val test = makeConnection.use(protocols.initialProtocol.run(_)).map(_.flatten)
          assertM(test.provideLayer(defaultEnv))(isSome(equalTo(remoteAddress)))
        }
      },
      suite("on receiving a neighbor message")(
        testM("should reject if not high priority and activeView is full") {
          checkM(gens.nodeAddress, gens.nodeAddress) {
            case (localAddress, remoteAddress) =>
              val makeConnection = {
                import MockConnection._
                make(
                  emit(Message.Neighbor(remoteAddress, false))
                    ++ await[Message](equalTo(Message.NeighborReject))
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
        testM("should reject if not high priority and activeView is not full") {
          checkM(gens.nodeAddress, gens.nodeAddress) {
            case (localAddress, remoteAddress) =>
              val makeConnection = {
                import MockConnection._
                make(
                  emit(Message.Neighbor(remoteAddress, false))
                    ++ await[Message](equalTo(Message.NeighborAccept))
                )
              }
              val test = makeConnection.use { con =>
                for {
                  protoResult <- protocols.initialProtocol.run(con)
                  viewsResult <- Views.passiveView.map(_.contains(remoteAddress)).commit
                } yield protoResult.map((_, viewsResult))
              }
              val env = defaultEnv >+> Views.live(localAddress, 1, 1)
              assertM(test.provideLayer(env))(isSome(equalTo((Some(remoteAddress), false))))
          }
        },
        testM("should accept if high priority and activeView is not full") {
          checkM(gens.nodeAddress, gens.nodeAddress) {
            case (localAddress, remoteAddress) =>
              val makeConnection = {
                import MockConnection._
                make(
                  emit(Message.Neighbor(remoteAddress, true))
                    ++ await[Message](equalTo(Message.NeighborAccept))
                )
              }
              val test = makeConnection.use { con =>
                for {
                  protoResult <- protocols.initialProtocol.run(con)
                  viewsResult <- Views.passiveView.map(_.contains(remoteAddress)).commit
                } yield protoResult.map((_, viewsResult))
              }
              val env = defaultEnv >+> Views.live(localAddress, 1, 1)
              assertM(test.provideLayer(env))(isSome(equalTo((Some(remoteAddress), false))))
          }
        },
        testM("should accept if high priority and activeView is full") {
          checkM(gens.nodeAddress, gens.nodeAddress) {
            case (localAddress, remoteAddress) =>
              val makeConnection = {
                import MockConnection._
                make(
                  emit(Message.Neighbor(remoteAddress, true))
                    ++ await[Message](equalTo(Message.NeighborAccept))
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
    TRandom.live >+> {
      Logging.ignore ++
        Views.live(address(0), 10, 10) ++
        HyParViewConfig.staticConfig(address(0), 10, 10, 5, 3, 2, 2, 3, 256, 256, 256)
    }

}
