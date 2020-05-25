package zio.keeper.membership.swim

import zio._
import zio.clock.Clock
import zio.keeper.ClusterError.UnknownNode
import zio.keeper.membership.MembershipEvent
import zio.keeper.membership.MembershipEvent.{ Join, Leave }
import zio.keeper.{ Error, NodeAddress }
import zio.logging._
import zio.stm.TMap
import zio.stream._

object Nodes {

  /**
   * Nodes maintains state of the cluster.
   */
  trait Service {
    def addNode(node: NodeAddress): UIO[Unit]

    /**
     * Changes node state and issue membership event.
     * @param id - member id
     * @param newState - new state
     */
    def changeNodeState(id: NodeAddress, newState: NodeState): IO[Error, Unit]

    /**
     * close connection and remove Node from cluster.
     * @param id node id
     */
    def disconnect(id: NodeAddress): IO[Error, Unit]

    /**
     *  Stream of Membership Events
     */
    def events: Stream[Nothing, MembershipEvent]

    /**
     *  Stream of Membership Events for internal purpose.
     *  This exists only because I cannot find the way to duplicates events from one queue
     */
    val internalEvents: Stream[Nothing, NodeStateChanged]

    /**
     * Returns next Healthy node.
     */
    val next: UIO[Option[NodeAddress]]

    /**
     * Node state for given NodeId.
     */
    def nodeState(id: NodeAddress): IO[Error, NodeState]

    val numberOfNodes: UIO[Int]

    /**
     * Lists members that are in healthy state.
     */
    def healthyNodes: UIO[List[(NodeAddress, NodeState)]]

    /**
     * Returns string with cluster state.
     */
    val prettyPrint: UIO[String]
  }

  def addNode(node: NodeAddress): ZIO[Nodes, Nothing, Unit] =
    ZIO.accessM[Nodes](_.get.addNode(node))

  val nextNode: URIO[Nodes, Option[NodeAddress]] =
    ZIO.accessM[Nodes](_.get.next)

  def nodeState(id: NodeAddress): ZIO[Nodes, Error, NodeState] =
    ZIO.accessM[Nodes](_.get.nodeState(id))

  def changeNodeState(id: NodeAddress, newState: NodeState): ZIO[Nodes, Error, Unit] =
    ZIO.accessM[Nodes](_.get.changeNodeState(id, newState))

  def disconnect(id: NodeAddress): ZIO[Nodes, Error, Unit] =
    ZIO.accessM[Nodes](_.get.disconnect(id))

  val internalEvents: ZStream[Nodes, Nothing, NodeStateChanged] =
    ZStream.accessStream[Nodes](_.get.internalEvents)

  val prettyPrint: URIO[Nodes, String] =
    ZIO.accessM[Nodes](_.get.prettyPrint)

  def events: ZStream[Nodes, Nothing, MembershipEvent] =
    ZStream.accessStream[Nodes](_.get.events)

  sealed trait NodeState

  object NodeState {
    case object Init        extends NodeState
    case object Healthy     extends NodeState
    case object Unreachable extends NodeState
    case object Suspicion   extends NodeState
    case object Dead        extends NodeState
    case object Left        extends NodeState
  }

  final case class NodeStateChanged(node: NodeAddress, oldState: NodeState, newState: NodeState)

  val live: ZLayer[Logging with Clock, Nothing, Nodes] =
    ZLayer.fromEffect(
      for {
        nodeStates          <- TMap.empty[NodeAddress, NodeState].commit
        eventsQueue         <- Queue.sliding[MembershipEvent](100)
        internalEventsQueue <- Queue.sliding[NodeStateChanged](100)
        roundRobinOffset    <- Ref.make(0)
        logger              <- ZIO.access[Logging](_.get)
      } yield new Nodes.Service {

        def addNode(node: NodeAddress): UIO[Unit] =
          nodeStates
            .put(node, NodeState.Init)
            .whenM(nodeStates.contains(node).map(!_))
            .commit
            .unit

        def changeNodeState(id: NodeAddress, newState: NodeState): IO[Error, Unit] =
          nodeState(id)
            .flatMap {
              prev =>
                ZIO.when(prev != newState) {
                  logger.info(s"changing node[$id] status from: [$prev] to: [$newState]") *>
                    nodeStates
                      .put(id, newState)
                      .commit
                      .tap(
                        _ => {
                          ZIO.whenCase(newState) {
                            case NodeState.Healthy if prev == NodeState.Init => eventsQueue.offer(Join(id))
                            case NodeState.Dead | NodeState.Left             => eventsQueue.offer(Leave(id))
                          } *> internalEventsQueue.offer(NodeStateChanged(id, prev, newState)).unit
                        }
                      )
                }
            }

        def disconnect(id: NodeAddress): IO[Error, Unit] =
          nodeStates.delete(id).commit

        def events: Stream[Nothing, MembershipEvent] =
          ZStream.fromQueue(eventsQueue)

        val internalEvents: Stream[Nothing, NodeStateChanged] =
          ZStream.fromQueue(internalEventsQueue)

        val next: UIO[Option[NodeAddress]] /*(exclude: List[NodeId] = Nil)*/ =
          for {
            list      <- healthyNodes
            nextIndex <- roundRobinOffset.updateAndGet(old => if (old < list.size - 1) old + 1 else 0)
            _         <- nodeStates.removeIf((_, v) => v == NodeState.Dead).when(nextIndex == 0).commit
          } yield list.drop(nextIndex).headOption.map(_._1)

        def nodeState(id: NodeAddress): IO[Error, NodeState] =
          nodeStates.get(id).commit.get.orElseFail(UnknownNode(id))

        def numberOfNodes: UIO[Int] =
          nodeStates.keys.map(_.size).commit

        def healthyNodes: UIO[List[(NodeAddress, NodeState)]] =
          nodeStates.toList.map(_.filter(_._2 == NodeState.Healthy)).commit

        val prettyPrint: UIO[String] =
          nodeStates.toList.commit.map(
            nodes =>
              "[ size: " + nodes.size +
                " nodes: [" +
                nodes
                  .map {
                    case (address, nodeState) =>
                      "address: " + address + " state: " + nodeState
                  }
                  .mkString("|") +
                "]]"
          )
      }
    )

}
