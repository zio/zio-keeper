package zio.keeper.membership.swim

import zio.ZIO
import zio.keeper.Error
import zio.keeper.membership.NodeId

trait Protocol[M] {

  def onMessage: PartialFunction[(NodeId, M), ZIO[Any, Error, Option[(NodeId, M)]]]

  def produceMessages: zio.stream.ZStream[Any, Error, (NodeId, M)]

}

