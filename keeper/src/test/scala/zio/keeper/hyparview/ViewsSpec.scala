package zio.keeper.hyparview

import zio._
import zio.keeper.SendError.TransportFailed
import zio.keeper.{ NodeAddress, TransportError, gens }
import zio.stm._
import zio.test.Assertion._
import zio.test._
import zio.test.environment.TestRandom

object ViewsSpec extends DefaultRunnableSpec {

  def address(n: Int): NodeAddress =
    NodeAddress(Array.emptyByteArray, n)

  def make(
    myself: NodeAddress,
    activeCapacity: Int,
    passiveCapacity: Int,
    seed: Long = 0L
  ): ZManaged[TestRandom with TRandom, Nothing, Views] =
    for {
      _     <- TestRandom.setSeed(seed).toManaged_
      views <- Views.live(myself, activeCapacity, passiveCapacity).build
    } yield views

  def spec =
    suite("ViewsSpec")(
      testM("adding the same node twice to the active view fails with ()") {
        checkM(gens.nodeAddress) {
          case x =>
            val result = make(address(0), 2, 2).use { views =>
              STM.atomically {
                for {
                  _ <- views.get.addToActiveView(x, _ => UIO.unit, UIO.unit)
                  _ <- views.get.addToActiveView(x, _ => UIO.unit, UIO.unit)
                } yield ()
              }
            }
            assertM(result.run)(fails(equalTo(())))
        }
      },
      testM("adding more than the maximum number of nodes to the active view fails with ()") {
        val gen = for {
          x1 <- gens.nodeAddress
          x2 <- gens.nodeAddress.filter(_ != x1)
          x3 <- gens.nodeAddress.filter(x => (x != x1) && (x != x2))
        } yield (x1, x2, x3)
        checkM(gen) {
          case (x1, x2, x3) =>
            val result = make(address(0), 2, 2).use { views =>
              STM.atomically {
                for {
                  _ <- views.get.addToActiveView(x1, _ => UIO.unit, UIO.unit)
                  _ <- views.get.addToActiveView(x2, _ => UIO.unit, UIO.unit)
                  _ <- views.get.addToActiveView(x3, _ => UIO.unit, UIO.unit)
                } yield ()
              }
            }
            assertM(result.run)(fails(equalTo(())))
        }
      },
      testM("adding the same node twice to the passive view is a noop") {
        checkM(gens.nodeAddress) { x =>
          make(address(0), 2, 2).use { views =>
            STM.atomically {
              for {
                _     <- views.get.addToPassiveView(x)
                size1 <- views.get.passiveViewSize
                _     <- views.get.addToPassiveView(x)
                size2 <- views.get.passiveViewSize
              } yield assert(size1)(equalTo(1)) && assert(size2)(equalTo(1))
            }
          }
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
            make(address(0), 2, 2).use { views =>
              STM.atomically {
                for {
                  _     <- views.get.addToPassiveView(x1)
                  _     <- views.get.addToPassiveView(x2)
                  size1 <- views.get.passiveViewSize
                  _     <- views.get.addToPassiveView(x3)
                  size2 <- views.get.passiveViewSize
                } yield assert(size1)(equalTo(2)) && assert(size2)(equalTo(2))
              }
            }
        }
      },
      testM("failing send with a TransportFailed calls disconnect on the node") {
        checkM(gens.nodeAddress) {
          case x =>
            Ref.make(0).flatMap { ref =>
              make(address(0), 2, 2).use { views =>
                for {
                  _ <- views.get
                        .addToActiveView(
                          x,
                          _ => ZIO.fail(TransportFailed(TransportError.ExceptionWrapper(new RuntimeException()))),
                          ref.update(_ + 1).unit
                        )
                        .commit
                  _      <- views.get.send(x, ActiveProtocol.Disconnect(address(1), false)).ignore
                  result <- assertM(ref.get)(equalTo(1))
                } yield result
              }
            }
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
            make(address(0), 2, 2).use { views =>
              STM.atomically {
                for {
                  _      <- views.get.addToPassiveView(x1)
                  _      <- views.get.addShuffledNodes(Set.empty, Set(x2, x3))
                  result <- views.get.passiveView
                } yield assert(result)(equalTo(Set(x2, x3)))
              }
            }
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
            make(address(0), 2, 3).use { views =>
              STM.atomically {
                for {
                  _      <- views.get.addShuffledNodes(Set(x1, x2), Set(x3, x4))
                  result <- views.get.passiveView
                } yield assert(result)(contains(x3) && contains(x4) && hasSize(equalTo(3)))
              }
            }
        }
      }
    ).provideCustomLayer(TRandom.live)
}
