package zio.keeper.hyparview

import zio._
import zio.keeper.{ KeeperSpec, NodeAddress, gens }
import zio.stm._
import zio.test.Assertion._
import zio.test._
import zio.clock.Clock
import zio.random.Random

object ViewsSpec extends KeeperSpec {

  def spec =
    suite("ViewsSpec")(
      testM("adding the same node twice to the active view evicts the old instance") {
        checkM(gens.nodeAddress) {
          case x =>
            val result = ZSTM
              .atomically {
                for {
                  ref    <- TRef.make(0)
                  _      <- Views.addToActiveView(x, _ => STM.unit, ref.update(_ + 1))
                  _      <- Views.addToActiveView(x, _ => STM.unit, ref.update(_ + 1))
                  result <- ref.get
                } yield result
              }
              .provideLayer(makeLayer(address(0), 2, 2))
            assertM(result)(equalTo((1)))
        }
      },
      testM("adding more than the maximum number of nodes to the active view drops nodes") {
        val gen = for {
          x1 <- gens.nodeAddress
          x2 <- gens.nodeAddress.filter(_ != x1)
          x3 <- gens.nodeAddress.filter(x => (x != x1) && (x != x2))
        } yield (x1, x2, x3)
        checkM(gen) {
          case (x1, x2, x3) =>
            val result = ZSTM
              .atomically {
                for {
                  ref    <- TRef.make(false)
                  _      <- Views.addToActiveView(x1, _ => STM.unit, ref.set(true))
                  _      <- Views.addToActiveView(x2, _ => STM.unit, ref.set(true))
                  _      <- Views.addToActiveView(x3, _ => STM.unit, ref.set(true))
                  result <- ref.get
                } yield result
              }
              .provideLayer(makeLayer(address(0), 2, 2))
            assertM(result)(isTrue)
        }
      },
      testM("adding the same node twice to the passive view is a noop") {
        checkM(gens.nodeAddress) { x =>
          ZSTM
            .atomically {
              for {
                _     <- Views.addToPassiveView(x)
                size1 <- Views.passiveViewSize
                _     <- Views.addToPassiveView(x)
                size2 <- Views.passiveViewSize
              } yield assert(size1)(equalTo(1)) && assert(size2)(equalTo(1))
            }
            .provideLayer(makeLayer(address(0), 2, 2))
        }
      },
      testM("adding more than the maximum number of nodes to the passive view drops nodes") {
        val gen = for {
          x1 <- gens.nodeAddress
          x2 <- gens.nodeAddress.filter(_ != x1)
          x3 <- gens.nodeAddress.filter(x => (x != x1) && (x != x2))
        } yield (x1, x2, x3)
        checkM(gen) {
          case (x1, x2, x3) =>
            ZSTM
              .atomically {
                for {
                  _     <- Views.addToPassiveView(x1)
                  _     <- Views.addToPassiveView(x2)
                  size1 <- Views.passiveViewSize
                  _     <- Views.addToPassiveView(x3)
                  size2 <- Views.passiveViewSize
                } yield assert(size1)(equalTo(2)) && assert(size2)(equalTo(2))
              }
              .provideLayer(makeLayer(address(0), 2, 2))
        }
      },
      testM("addShuffledNodes will add all nodes in the replied set") {
        val gen = for {
          x1 <- gens.nodeAddress
          x2 <- gens.nodeAddress.filter(_ != x1)
          x3 <- gens.nodeAddress.filter(x => (x != x1) && (x != x2))
        } yield (x1, x2, x3)
        checkM(gen) {
          case (x1, x2, x3) =>
            ZSTM
              .atomically {
                for {
                  _      <- Views.addToPassiveView(x1)
                  _      <- protocols.addShuffledNodes(Set.empty, Set(x2, x3))
                  result <- Views.passiveView
                } yield assert(result)(equalTo(Set(x2, x3)))
              }
              .provideLayer(makeLayer(address(0), 2, 2))
        }
      },
      testM("addShuffledNodes will add nodes from sentOriginally if there is space") {
        val gen = for {
          x1 <- gens.nodeAddress
          x2 <- gens.nodeAddress.filter(_ != x1)
          x3 <- gens.nodeAddress.filter(x => (x != x1) && (x != x2))
          x4 <- gens.nodeAddress.filter(x => (x != x1) && (x != x2) && (x != x3))
        } yield (x1, x2, x3, x4)
        checkM(gen) {
          case (x1, x2, x3, x4) =>
            ZSTM
              .atomically {
                for {
                  _      <- protocols.addShuffledNodes(Set(x1, x2), Set(x3, x4))
                  result <- Views.passiveView
                } yield assert(result)(contains(x3) && contains(x4) && hasSize(equalTo(3)))
              }
              .provideLayer(makeLayer(address(0), 2, 3))
        }
      }
    )

  def makeLayer(
    myself: NodeAddress,
    activeCapacity: Int,
    passiveCapacity: Int
  ): ZLayer[Random with Clock with Sized, Nothing, Views with TRandom] =
    ZLayer.identity[Random with Clock with Sized] >+>
      TRandom.live >+>
      HyParViewConfig.static(myself, activeCapacity, passiveCapacity, 0, 0, 0, 0, 0, 0, 0, 0, 256) >+>
      Views.live
}
