package zio.membership.hyparview

private class PRNG private (private val seed: Long) extends AnyVal {
  import PRNG._

  def next: (Int, PRNG) = {
    val nextSeed = (seed.toLong * Multiplier + Addend) & Mask
    ((nextSeed >>> 16).toInt, new PRNG(nextSeed))
  }
}

private object PRNG {

  val Multiplier = 0x5DEECE66DL
  val Addend     = 0xBL
  val Mask       = (1L << 48) - 1

  def scramble(seed: Long): Long = (seed ^ Multiplier) & Mask

  def make(seed: Long): PRNG = new PRNG(scramble(seed))
}
