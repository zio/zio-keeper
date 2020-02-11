package zio.keeper.membership.swim.protocols

import java.util.concurrent.TimeUnit

import zio._
import zio.clock.{ Clock, _ }
import zio.duration._
import zio.keeper.TaggedCodec
import zio.keeper.membership.NodeId
import zio.keeper.membership.swim.protocols.FailureDetection.{ Ack, Ping, PingReq }
import zio.keeper.membership.swim.{ GossipState, Protocol }
import zio.keeper.transport.Connection
import zio.stm.TMap
import zio.stream.ZStream

trait Runner[A] extends Protocol[FailureDetection[A]] {

  case class _Ack(nodeId: NodeId, timestamp: Long)

  val acks: TMap[Long, _Ack]
  val state: Ref[GossipState[A]]
  val roundRobinOffset: Ref[Int]
  val nodeChannels: Ref[Map[NodeId, Connection]]
  val ackId: Ref[Long]

  val dependencies: UIO[Clock]

  implicit val codec: TaggedCodec[FailureDetection[A]]

  private def nextNode =
    for {
      nodes     <- nodeChannels.get
      nextIndex <- roundRobinOffset.update(old => if (old < nodes.size - 1) old + 1 else 0)
    } yield nodes.keys.drop(nextIndex).headOption

  private def updateState(newState: GossipState[A]) =
    for {
      current <- state.get
      diff    = newState.diff(current)
//        _       <- ZIO.foreach(diff.local)(n => (n.addr >>= connect).ignore)
    } yield ()

  private def withAck(fn: Long => (NodeId, FailureDetection[A])) =
    for {
      ackId      <- ackId.update(_ + 1)
      timestamp  <- currentTime(TimeUnit.MILLISECONDS)
      nodeAndMsg = fn(ackId)
      _          <- acks.put(ackId, _Ack(nodeAndMsg._1, timestamp)).commit
    } yield nodeAndMsg

  private def ack(id: Long) =
    acks
      .delete(id)
      .commit

  override def onMessage = {
    case (_, Ack(ackId, state)) =>
      updateState(state.asInstanceOf[GossipState[A]]) *>
        ack(ackId)
          .as(
            noReply
          )

    case (_, Ping(ackId, state0)) =>
      updateState(state0.asInstanceOf[GossipState[A]]) *>
        state.get.flatMap(state => reply(Ack[A](ackId, state)))

    case (_, PingReq(to, ackId, state0)) =>
      updateState(state0.asInstanceOf[GossipState[A]]) *>
        state.get
          .flatMap(state => forward[FailureDetection[A]](to, Ack[A](ackId, state)))
          .flatMap(r => withAck(ackId, r, 5.seconds))

  }

  override def produceMessages: ZStream[Any, keeper.Error, (NodeId, FailureDetection[A])] =
    ZStream
      .fromIterator(
        acks.toList.commit.map(_.iterator)
      )
      .zip(ZStream.repeatEffectWith(currentTime(TimeUnit.MICROSECONDS), Schedule.spaced(1.second)))
      .collectM {
        case ((ackId, ack), current) if current - ack.timestamp > 5.seconds.toMillis =>
          nextNode
            .zip(state.get)
            .map {
              case (node, state) =>
                node.map(next => (next, PingReq(ack.nodeId, ackId, state)))
            }
      }
      .collect {
        case Some(req) => req
      }
      .merge(
        ZStream
          .repeatEffectWith(
            nextNode <*> state.get,
            Schedule.spaced(3.seconds)
          )
          .collectM {
            case (Some(next), state) =>
              withAck(ackId => (next, Ping(ackId, state)))
          }
      )
      .provideM(dependencies)

  override def onError(err: keeper.Error): ZIO[Any, keeper.Error, Unit] = ???
}
