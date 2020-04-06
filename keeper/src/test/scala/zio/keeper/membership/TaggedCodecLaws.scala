package zio.keeper.membership

import zio.test.Assertion._
import zio.test._

import scala.reflect.runtime.universe._

object TaggedCodecLaws {

  final class TaggedCodecLawsPartiallyApplied[A] {

    def apply[R](gen: Gen[R, A])(
      implicit
      codec: TaggedCodec[A],
      tag: TypeTag[A]
    ): Spec[R, TestFailure[Nothing], TestSuccess] =
      suite(s"TaggedCodecLaws[${typeOf[A].typeSymbol.name.toString}]")(
        testM("round trip using tagged ByteCodec") {
          checkM(gen) { a =>
            val tag = codec.tagOf(a)
            val test = for {
              byteCodec <- codec.codecFor(tag)
              chunk     <- byteCodec.toChunk(a)
              decoded   <- byteCodec.fromChunk(chunk)
            } yield decoded
            assertM(test.run)(succeeds(equalTo(a)))
          }
        }
      )
  }

  def apply[A]: TaggedCodecLawsPartiallyApplied[A] =
    new TaggedCodecLawsPartiallyApplied[A]()
}
