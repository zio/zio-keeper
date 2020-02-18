package zio.keeper.membership.swim

import zio.keeper.TaggedCodec

case class EnrichedMessage[A, B](msg: A, piggyBaked: List[B])

object EnrichedMessage {
  implicit def tagged[A: TaggedCodec, B: TaggedCodec]: TaggedCodec[EnrichedMessage[A, B]] = ???
}
