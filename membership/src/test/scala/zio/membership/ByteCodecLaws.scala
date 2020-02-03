package zio.membership

import zio.test._
import zio.test.Assertion._
import scala.reflect.runtime.universe._

object ByteCodecLaws {

  final class ByteCodecLawsPartiallyApplied[A] {

    def apply[E](gen: Gen[E, A])(
      implicit
      codec: ByteCodec[A],
      tag: TypeTag[A]
    ): Spec[E, TestFailure[Nothing], String, TestSuccess[Unit]] =
      suite(s"ByteCodecLaws[${typeOf[A].typeSymbol.name.toString}]")(
        testM("codec round trip") {
          checkM(gen) { a =>
            assertM(
              codec.toChunk(a).flatMap[Any, Any, A](codec.fromChunk).run,
              succeeds(equalTo(a))
            )
          }
        }
      )
  }

  def apply[A]: ByteCodecLawsPartiallyApplied[A] =
    new ByteCodecLawsPartiallyApplied[A]()
}
