package zio.keeper.membership.swim.protocols

import java.util.concurrent.TimeUnit

import zio._
import zio.clock.{ Clock, _ }
import zio.duration._
import zio.keeper.membership.NodeId
import zio.keeper.membership.swim.protocols.FailureDetection.{ Ack, Ping, PingReq }
import zio.keeper.membership.swim.{ GossipState, Nodes, Protocol }
import zio.stm.TMap
import zio.stream.ZStream

object FailureDetectionProtocol {
  case class _Ack(nodeId: NodeId, timestamp: Long, onBehalf: Option[(NodeId, Long)])

  def start[A](nodes: Nodes[A]) =
    for {
      deps  <- ZIO.environment[Clock]
      acks  <- TMap.empty[Long, _Ack].commit
      ackId <- Ref.make(0L)
      state <- Ref.make(GossipState.Empty[A])

    } yield new Protocol[FailureDetection[A]] {

      private def ack(id: Long) =
        (acks.get(id) <*
          acks
            .delete(id)).commit

      override def onMessage = {
        case (_, Ack(ackId, state)) =>
          updateState(state.asInstanceOf[GossipState[A]]) *>
            ack(ackId).map {
              case Some(_Ack(_, _, Some((node, originalAckId)))) =>
                Some((node, Ack(originalAckId, state)))
              case _ =>
                None
            }

        case (sender, Ping(ackId, state0)) =>
          updateState(state0.asInstanceOf[GossipState[A]]) *>
            state.get.map(state => Some((sender, Ack[A](ackId, state))))

        case (sender, PingReq(to, originalAck, state0)) =>
          updateState(state0.asInstanceOf[GossipState[A]]) *>
            state.get
              .flatMap(state => withAck(Some((sender, originalAck)), ackId => (to, Ping(ackId, state))))
              .map(Some(_))
              .provide(deps)

      }

      override def produceMessages: ZStream[Any, keeper.Error, (NodeId, FailureDetection[A])] =
        ZStream
          .fromIterator(
            acks.toList.commit.map(_.iterator)
          )
          .zip(ZStream.repeatEffectWith(currentTime(TimeUnit.MICROSECONDS), Schedule.spaced(1.second)))
          .collectM {
            case ((ackId, ack), current) if current - ack.timestamp > 5.seconds.toMillis =>
              nodes.next
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
                nodes.next <*> state.get,
                Schedule.spaced(3.seconds)
              )
              .collectM {
                case (Some(next), state) =>
                  withAck(None, ackId => (next, Ping(ackId, state)))
              }
          )
          .provide(deps)

      private def updateState(newState: GossipState[A]) =
        for {
          current <- state.get
          diff    = newState.diff(current)
          _       <- ZIO.foreach(diff.local)(n => nodes.connect(n.nodeId, n.addr).ignore)
        } yield ()

      private def withAck(onBehalf: Option[(NodeId, Long)], fn: Long => (NodeId, FailureDetection[A])) =
        for {
          ackId      <- ackId.update(_ + 1)
          timestamp  <- currentTime(TimeUnit.MILLISECONDS)
          nodeAndMsg = fn(ackId)
          _          <- acks.put(ackId, _Ack(nodeAndMsg._1, timestamp, onBehalf)).commit
        } yield nodeAndMsg

    }
}
