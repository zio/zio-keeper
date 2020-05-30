package zio.keeper.hyparview.plumtree

import zio.keeper.KeeperSpec
import zio.stm.STM
import zio.test.Assertion._
import zio.test._

object BoundedTMapSpec extends KeeperSpec {

  val spec = suite("BoundedTMap")(
    testM("returns true for elements not seen before") {
      checkM(Gen.anyInt) { key =>
        for {
          m      <- BoundedTMap.make[Int, Unit](10)
          result <- m.add(key, ()).commit
        } yield assert(result)(isTrue)
      }
    },
    testM("returns false for elements seen before") {
      checkM(Gen.anyInt) { key =>
        for {
          m      <- BoundedTMap.make[Int, Unit](10)
          result <- (m.add(key, ()) *> m.add(key, ())).commit
        } yield assert(result)(isFalse)
      }
    },
    testM("Forgets messages after buffer other elements have been seen") {
      val gen = for {
        elem1 <- Gen.anyInt
        elem2 <- Gen.anyInt.filter(_ != elem1)
        elem3 <- Gen.anyInt.filter(e => (e != elem1) && (e != elem2))
        elem4 <- Gen.anyInt.filter(e => (e != elem1) && (e != elem2) && (e != elem3))
      } yield (elem1, elem2, elem3, elem4)

      checkM(gen) {
        case (key1, key2, key3, key4) =>
          for {
            m <- BoundedTMap.make[Int, Unit](3)
            result <- STM.atomically {
                       m.add(key1, ()) *>
                         m.add(key2, ()) *>
                         m.add(key3, ()) *>
                         m.add(key4, ()) *>
                         m.add(key1, ())
                     }
          } yield assert(result)(isTrue)
      }
    }
  )
}
