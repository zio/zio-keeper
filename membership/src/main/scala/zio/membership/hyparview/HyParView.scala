package zio.membership.hyparview

import zio.membership.Membership
import zio._
import zio.membership.transport.Transport
import zio.random.Random

object HyParView {

  def apply[T](
    localAddr: T,
    activeViewCapactiy: Int,
    passiveViewCapacity: Int,
    arwl: Int,
    prwl: Int
  ): ZManaged[Transport[T] with Random, Nothing, Membership[T]] = ???

}
