package zio.membership

import zio._
import zio.stream._

/**
 * The main entrypoint to the membership library. Allows querying the cluster state
 * and sending messages to members.
 */
trait Membership {
  val membership: Membership.Service[Any]
}

object Membership {

  trait Service[R] {

    /**
     * Send a message to all nodes.
     */
    def broadcast[R1 <: R, A](payload: A)(implicit ev: ByteCodec[R1, A]): ZIO[R1, Error, Unit]

    /**
     * Get a list of all nodes that are currently considered healthy.
     * Note that depending on implementation this might only return the nodes
     * in a local view.
     */

    val nodes: ZIO[R, Nothing, List[Member]]

    /**
     * Receive a stream of all messages.
     */

    val receive: ZStream[R, Error, Message]

    /**
     * Send a message to a node.
     */
    def send[R1 <: R, A](to: Member, payload: A)(implicit ev: ByteCodec[R1, A]): ZIO[R1, Error, Unit]
  }
}
