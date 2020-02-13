package zio.keeper.membership.swim

import zio.ZIO
import zio.keeper.Error

trait Protocol[A, M] {
  self =>

  def onMessage: PartialFunction[(A, M), ZIO[Any, Error, Option[(A, M)]]]

  def produceMessages: zio.stream.ZStream[Any, Error, (A, M)]

}

