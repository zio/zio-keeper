package zio.keeper

import zio.test.Assertion._
import zio.test._

import scala.reflect.runtime.universe._

object ByteCodecLaws {

  final class ByteCodecLawsPartiallyApplied[A] {

    def apply[R](gen: Gen[R, A])(
      implicit
      codec: ByteCodec[A],
      tag: TypeTag[A]
    ): Spec[R, TestFailure[Nothing], TestSuccess] =
      suite(s"ByteCodecLaws[${typeOf[A].typeSymbol.name.toString}]")(
        testM("codec round trip") {
          checkM(gen) { a =>
            assertM(codec.toChunk(a).flatMap[Any, Any, A](codec.fromChunk).run)(
              succeeds(equalTo(a))
            )
          }
        }
      )
  }

  def apply[A]: ByteCodecLawsPartiallyApplied[A] =
    new ByteCodecLawsPartiallyApplied[A]()
}
