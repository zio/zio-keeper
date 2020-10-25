package zio.keeper.hyparview

import zio.stm._
import zio.test._
import zio.test.Assertion._
import zio.keeper.KeeperSpec
import zio.keeper.gens
import zio.keeper.transport.testing.MockConnection._
import zio.logging.Logging
import zio.ZLayer
import zio.keeper.NodeAddress
import zio.random.Random
import zio.clock.Clock

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
                emit(Message.Join(remoteAddress))
              }
              makeConnection
                .use { con =>
                  for {
                    protoResult <- protocols.initialProtocol.run(con)
                  } yield assert(protoResult.flatten)(isSome(equalTo(remoteAddress)))
                }
                .provideLayer(env(localAddress, 1, 1))
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
                emit(Message.Join(remoteAddress))
              }
              makeConnection
                .use { con =>
                  for {
                    ref    <- TRef.make[List[Message]](Nil).commit
                    _      <- Views.addToActiveView(existingAddress, m => ref.update(m :: _), STM.unit).commit
                    _      <- protocols.initialProtocol.run(con)
                    result <- ref.get.commit
                  } yield assert(result)(equalTo(List(Message.ForwardJoin(remoteAddress, TimeToLive(5)))))
                }
                .provideLayer(env(localAddress, 10, 10))
          }
        }
      ),
      testM("succeeds with sender on receiving ForwardJoinReply") {
        checkM(gens.nodeAddress) { remoteAddress =>
          val makeConnection = {
            emit(Message.ForwardJoinReply(remoteAddress))
          }
          makeConnection
            .use { con =>
              for {
                result <- protocols.initialProtocol.run(con)
              } yield assert(result.flatten)(isSome(equalTo(remoteAddress)))
            }
            .provideLayer(defaultEnv)
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
                emit(Message.Neighbor(remoteAddress, false)) ++ await[Message](equalTo(Message.NeighborReject))
              }
              makeConnection
                .use { con =>
                  for {
                    _           <- Views.addToActiveView(existingAddress, _ => STM.unit, STM.unit).ignore.commit
                    protoResult <- protocols.initialProtocol.run(con)
                    viewsResult <- Views.passiveView.map(_.contains(remoteAddress)).commit
                  } yield assert(protoResult.map((_, viewsResult)))(isSome(equalTo((None, true))))
                }
                .provideLayer(env(localAddress, 1, 1))
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
                emit(Message.Neighbor(remoteAddress, false)) ++ await[Message](equalTo(Message.NeighborAccept))
              }
              makeConnection
                .use { con =>
                  for {
                    protoResult <- protocols.initialProtocol.run(con)
                    viewsResult <- Views.passiveView.map(_.contains(remoteAddress)).commit
                  } yield assert(protoResult.map((_, viewsResult)))(isSome(equalTo((Some(remoteAddress), false))))
                }
                .provideLayer(env(localAddress, 1, 1))
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
                emit(Message.Neighbor(remoteAddress, true))
              }
              makeConnection
                .use { con =>
                  for {
                    protoResult <- protocols.initialProtocol.run(con)
                    viewsResult <- Views.passiveView.map(_.contains(remoteAddress)).commit
                  } yield assert(protoResult.map((_, viewsResult)))(isSome(equalTo((Some(remoteAddress), false))))
                }
                .provideLayer(env(localAddress, 1, 1))
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
                emit(Message.Neighbor(remoteAddress, true))
              }
              makeConnection
                .use { con =>
                  for {
                    _           <- Views.addToActiveView(existingAdress, _ => STM.unit, STM.unit).ignore.commit
                    protoResult <- protocols.initialProtocol.run(con)
                    viewsResult <- Views.passiveView.map(_.contains(remoteAddress)).commit
                  } yield assert(protoResult.map((_, viewsResult)))(isSome(equalTo((Some(remoteAddress), false))))
                }
                .provideLayer(env(localAddress, 1, 1))
          }
        }
      )
    )

  private val defaultEnv: ZLayer[Random with Clock, Nothing, TRandom with HyParViewConfig with Logging with Views] =
    env(address(0), 10, 10)

  private def env(
    address: NodeAddress,
    activeViewCapacity: Int,
    passiveViewCapacity: Int
  ): ZLayer[Random with Clock, Nothing, TRandom with HyParViewConfig with Logging with Views] =
    ZLayer.identity[Clock with Random] >+>
      TRandom.live >+> HyParViewConfig.static(
      address,
      activeViewCapacity,
      passiveViewCapacity,
      5,
      3,
      2,
      2,
      3,
      256,
      10,
      10
    ) >+> Logging.ignore >+> Views.live
}
