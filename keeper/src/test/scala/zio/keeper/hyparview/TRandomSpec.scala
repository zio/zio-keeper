package zio.keeper.hyparview

import zio.ZManaged
import zio.random.Random
import zio.test.Assertion._
import zio.test._
import zio.test.environment.TestRandom

object TRandomSpec extends DefaultRunnableSpec {

  def make(seed: Long): ZManaged[Random with TestRandom, Nothing, TRandom] =
    for {
      _     <- TestRandom.setSeed(seed).toManaged_
      trand <- TRandom.live.build
    } yield trand

  def spec = suite("TRandomSpec")(
    suite("selectOne")(
      testM("will draw an element from the list") {
        checkM(Gen.listOfN(10)(Gen.anyInt)) { xs =>
          make(0).use(_.get.selectOne(xs).commit).map { element =>
            assert(xs)(contains(element.get))
          }
        }
      },
      testM("will draw the same element with the same seed") {
        checkM(Gen.listOf(Gen.anyInt)) { xs =>
          val draw = make(0).use(_.get.selectOne(xs).commit)
          for {
            fst <- draw
            snd <- draw
          } yield assert(fst)(equalTo(snd))
        }
      },
      testM("will draw different elements with different seeds") {
        checkM(Gen.listOfN(10)(Gen.anyInt)) { xs =>
          for {
            fst <- make(0).use(_.get.selectOne(xs).commit)
            snd <- make(1).use(_.get.selectOne(xs).commit)
          } yield assert(fst)(not(equalTo(snd)))
        }
      },
      testM("returns None for empty list") {
        assertM(make(0).use(_.get.selectOne(Nil).commit))(isNone)
      }
    ),
    suite("selectN")(
      testM("returns xs.size elements with n is larger") {
        val gen = for {
          xs <- Gen.listOf(Gen.anyInt)
          n  <- Gen.int(xs.size + 1, Int.MaxValue)
        } yield (xs, n)
        checkM(gen) {
          case (xs, n) =>
            assertM(make(0).use(_.get.selectN(xs, n).map(_.size).commit))(equalTo(xs.size))
        }
      },
      testM("returns n elements with n is smaller") {
        val gen = for {
          xs <- Gen.listOf(Gen.anyInt)
          n  <- Gen.int(0, xs.size)
        } yield (xs, n)
        checkM(gen) {
          case (xs, n) =>
            assertM(make(0).use(_.get.selectN(xs, n).map(_.size).commit))(equalTo(n))
        }
      },
      testM("returns 0 elements when n <= 0") {
        val gen = for {
          xs <- Gen.listOf(Gen.anyInt)
          n  <- Gen.int(Int.MinValue + 10, 0)
        } yield (xs, n)
        checkM(gen) {
          case (xs, n) =>
            assertM(make(0).use(_.get.selectN(xs, n).map(_.size).commit))(equalTo(0))
        }
      },
      testM("returns the same elements as the collection") {
        checkM(Gen.listOf(Gen.anyInt)) { xs =>
          assertM(make(0).use(_.get.selectN(xs, xs.size).map(_.sorted).commit))(equalTo(xs.sorted))
        }
      }
    )
  )
}
