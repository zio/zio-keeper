package zio.keeper.hyparview

import zio.stm._
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
          val gen = for {
            localAddress  <- gens.nodeAddress
            remoteAddress <- gens.nodeAddress.filter(_ != localAddress)
          } yield (localAddress, remoteAddress)
          checkM(gen) {
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
        testM("should send ForwardJoin to every other member of active view") {
          val gen = for {
            localAddress    <- gens.nodeAddress
            remoteAddress   <- gens.nodeAddress.filter(_ != localAddress)
            existingAddress <- gens.nodeAddress.filter(a => (a != localAddress) && (a != remoteAddress))
          } yield (localAddress, remoteAddress, existingAddress)
          checkM(gen) {
            case (localAddress, remoteAddress, existingAddress) =>
              val makeConnection = {
                import MockConnection._
                make(
                  emit(Message.Join(remoteAddress)) ++
                    await[Message](equalTo(Message.JoinReply(localAddress)))
                )
              }
              val test = makeConnection.use { con =>
                for {
                  ref    <- TRef.make[List[Message]](Nil).commit
                  _      <- Views.addToActiveView(existingAddress, m => ref.update(m :: _), STM.unit).commit
                  _      <- protocols.initialProtocol.run(con)
                  result <- ref.get.commit
                } yield result
              }
              val env = defaultEnv >+> Views.live(localAddress, 10, 10)
              assertM(test.provideLayer(env))(equalTo(List(Message.ForwardJoin(remoteAddress, TimeToLive(5)))))
          }
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
          val gen = for {
            localAddress    <- gens.nodeAddress
            remoteAddress   <- gens.nodeAddress.filter(_ != localAddress)
            existingAddress <- gens.nodeAddress.filter(a => (a != localAddress) && (a != remoteAddress))
          } yield (localAddress, remoteAddress, existingAddress)
          checkM(gen) {
            case (localAddress, remoteAddress, existingAddress) =>
              val makeConnection = {
                import MockConnection._
                make(
                  emit(Message.Neighbor(remoteAddress, false))
                    ++ await[Message](equalTo(Message.NeighborReject))
                )
              }
              val test = makeConnection.use { con =>
                for {
                  _           <- Views.addToActiveView(existingAddress, _ => STM.unit, STM.unit).ignore.commit
                  protoResult <- protocols.initialProtocol.run(con)
                  viewsResult <- Views.passiveView.map(_.contains(remoteAddress)).commit
                } yield protoResult.map((_, viewsResult))
              }
              val env = defaultEnv >+> Views.live(localAddress, 1, 1)
              assertM(test.provideLayer(env))(isSome(equalTo((None, true))))
          }
        },
        testM("should reject if not high priority and activeView is not full") {
          val gen = for {
            localAddress  <- gens.nodeAddress
            remoteAddress <- gens.nodeAddress.filter(_ != localAddress)
          } yield (localAddress, remoteAddress)
          checkM(gen) {
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
          val gen = for {
            localAddress  <- gens.nodeAddress
            remoteAddress <- gens.nodeAddress.filter(_ != localAddress)
          } yield (localAddress, remoteAddress)
          checkM(gen) {
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
          val gen = for {
            localAddress    <- gens.nodeAddress
            remoteAddress   <- gens.nodeAddress.filter(_ != localAddress)
            existingAddress <- gens.nodeAddress.filter(a => (a != localAddress) && (a != remoteAddress))
          } yield (localAddress, remoteAddress, existingAddress)
          checkM(gen) {
            case (localAddress, remoteAddress, existingAdress) =>
              val makeConnection = {
                import MockConnection._
                make(
                  emit(Message.Neighbor(remoteAddress, true))
                    ++ await[Message](equalTo(Message.NeighborAccept))
                )
              }
              val test = makeConnection.use { con =>
                for {
                  _           <- Views.addToActiveView(existingAdress, _ => STM.unit, STM.unit).ignore.commit
                  protoResult <- protocols.initialProtocol.run(con)
                  viewsResult <- Views.passiveView.map(_.contains(remoteAddress)).commit
                } yield protoResult.map((_, viewsResult))
              }
              val env = defaultEnv >+> Views.live(localAddress, 1, 1)
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
