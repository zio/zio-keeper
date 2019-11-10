package zio.membership

import zio._
import zio.duration._
import zio.stream._

/**
 * The main entrypoint to the membership library. Allows querying the cluster state
 * and sending messages to members.
 *
 * TODO: do we want to use a codec typeclass instead of sending Chunk[Byte] here?
 */
trait Membership {
  val membership: Membership.Service[Any]
}

object Membership {

  trait Service[R] {

    /**
     * Get a list of all nodes that are currently considered healthy.
     */
    def nodes: ZIO[R, Nothing, List[NodeId]]

    /**
     * TODO: Should we use TCP/UDP/offer both here?
     * Send a messsage to a node. This will suspend until the message has been send.
     */
    def sendMessage(to: NodeId, data: Chunk[Byte]): ZIO[R, Error, Unit]

    /**
     * TODO: naming
     * Send a message to a node. This will suspend until the message has been send.
     * The returned message can be used to wait for the expected reply to this message and will
     * fail once the timeout has passed.
     */
    def sendMessageReply(to: NodeId, data: Chunk[Byte], timeout: Duration): ZIO[R, Error, Promise[Unit, Message]]

    /**
     * TODO: Do we want to offer this here or already full-fledged crdts?
     *
     * Register a broadcast that will periodically send messages to random other cluster nodes.
     * The returned function can be used to set a new value that should be broadcast.
     *
     * The broadcasted messages will usually be piggybacked on the gossip messages of the membership
     * protocol. If the gossip protocol would spread and a broadcasted message is ready for sending
     * it will be embedded in the gossip message. Therefore choosing a multiple of the broadcast period
     * of the gossip protocol leads to less network load here.
     *
     * Retransmits configures how often a new value should be propagated to cluster members. If a new value
     * is registered by the user the old message will no longer be propagated.
     */
    def registerBroadcast(period: Duration, retransmits: Int): ZManaged[R, Error, Chunk[Byte] => UIO[Unit]]

    /**
     * Receive a stream of all messages. Messages that are part of a conversation (send with
     * sendMessageReply) will not appear here.
     */
    def receive: ZStream[R, Error, Message]
  }
}
