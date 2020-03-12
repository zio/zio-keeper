package zio.membership.hyparview

import zio.test._
import zio.test.Assertion._
import zio.test.Gen
import zio.test.environment.TestRandom
import zio.stm._
import zio._
import zio.membership.SendError.TransportFailed
import zio.membership.TransportError

object ViewsSpec
    extends DefaultRunnableSpec({
      def make[T](
        myself: T,
        activeCapacity: Int,
        passiveCapacity: Int,
        seed: Long = 0L
      ): ZIO[TestRandom, Nothing, Views.Service[Any, T]] =
        (TestRandom.setSeed(seed) *>
          TRandom.make) >>> Views.make(myself, activeCapacity, passiveCapacity).map(_.views)

      suite("ViewsSpec")(
        testM("adding the same node twice to the active view fails with ()") {
          checkM(Gen.anyInt) {
            case x =>
              val result = make(0, 2, 2).flatMap { views =>
                STM.atomically {
                  for {
                    _ <- views.addToActiveView(x, _ => UIO.unit, UIO.unit)
                    _ <- views.addToActiveView(x, _ => UIO.unit, UIO.unit)
                  } yield ()
                }
              }
              assertM(result.run)(fails(equalTo(())))
          }
        },
        testM("adding more than the maximum number of nodes to the active view fails with ()") {
          val gen = for {
            x1 <- Gen.anyInt
            x2 <- Gen.anyInt.filter(_ != x1)
            x3 <- Gen.anyInt.filter(x => (x != x1) && (x != x2))
          } yield (x1, x2, x3)
          checkM(gen) {
            case (x1, x2, x3) =>
              val result = make(0, 2, 2).flatMap { views =>
                STM.atomically {
                  for {
                    _ <- views.addToActiveView(x1, _ => UIO.unit, UIO.unit)
                    _ <- views.addToActiveView(x2, _ => UIO.unit, UIO.unit)
                    _ <- views.addToActiveView(x3, _ => UIO.unit, UIO.unit)
                  } yield ()
                }
              }
              assertM(result.run)(fails(equalTo(())))
          }
        },
        testM("adding the same node twice to the passive view is a noop") {
          checkM(Gen.anyInt) {
            case x =>
              make(0, 2, 2).flatMap { views =>
                STM.atomically {
                  for {
                    _     <- views.addToPassiveView(x)
                    size1 <- views.passiveViewSize
                    _     <- views.addToPassiveView(x)
                    size2 <- views.passiveViewSize
                  } yield assert(size1)(equalTo(1)) && assert(size2)(equalTo(1))
                }
              }
          }
        },
        testM("adding more than the maximum number of nodes to the passive view drops nodes") {
          val gen = for {
            x1 <- Gen.anyInt
            x2 <- Gen.anyInt.filter(_ != x1)
            x3 <- Gen.anyInt.filter(x => (x != x1) && (x != x2))
          } yield (x1, x2, x3)
          checkM(gen) {
            case (x1, x2, x3) =>
              make(0, 2, 2).flatMap { views =>
                STM.atomically {
                  for {
                    _     <- views.addToPassiveView(x1)
                    _     <- views.addToPassiveView(x2)
                    size1 <- views.passiveViewSize
                    _     <- views.addToPassiveView(x3)
                    size2 <- views.passiveViewSize
                  } yield assert(size1)(equalTo(2)) && assert(size2)(equalTo(2))
                }
              }
          }
        },
        testM("failing send with a TransportFailed calls disconnect on the node") {
          checkM(Gen.anyInt) {
            case x =>
              Ref.make(0).flatMap { ref =>
                make(0, 2, 2).flatMap { views =>
                  for {
                    _ <- views
                          .addToActiveView(
                            x,
                            _ => ZIO.fail(TransportFailed(TransportError.ExceptionThrown(new RuntimeException()))),
                            ref.update(_ + 1).unit
                          )
                          .commit
                    _      <- views.send(x, ActiveProtocol.Disconnect(1, false)).ignore
                    result <- assertM(ref.get)(equalTo(1))
                  } yield result
                }
              }
          }
        },
        testM("addShuffledNodes will add all nodes in the replied set") {
          val gen = for {
            x1 <- Gen.anyInt
            x2 <- Gen.anyInt.filter(_ != x1)
            x3 <- Gen.anyInt.filter(x => (x != x1) && (x != x2))
          } yield (x1, x2, x3)
          checkM(gen) {
            case (x1, x2, x3) =>
              make(0, 2, 2).flatMap { views =>
                STM.atomically {
                  for {
                    _      <- views.addToPassiveView(x1)
                    _      <- views.addShuffledNodes(Set.empty, Set(x2, x3))
                    result <- views.passiveView
                  } yield assert(result)(equalTo(Set(x2, x3)))
                }
              }
          }
        },
        testM("addShuffledNodes will add nodes from sentOriginally if there is space") {
          val gen = for {
            x1 <- Gen.anyInt
            x2 <- Gen.anyInt.filter(_ != x1)
            x3 <- Gen.anyInt.filter(x => (x != x1) && (x != x2))
            x4 <- Gen.anyInt.filter(x => (x != x1) && (x != x2) && (x != x3))
          } yield (x1, x2, x3, x4)
          checkM(gen) {
            case (x1, x2, x3, x4) =>
              make(0, 2, 3).flatMap { views =>
                STM.atomically {
                  for {
                    _      <- views.addShuffledNodes(Set(x1, x2), Set(x3, x4))
                    result <- views.passiveView
                  } yield assert(result)(contains(x3) && contains(x4) && hasSize(equalTo(3)))
                }
              }
          }
        }
      )
    })
