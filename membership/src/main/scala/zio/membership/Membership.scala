package zio.membership

import zio._
import zio.stream._

/**
 * The main entrypoint to the membership library. Allows querying the cluster state
 * and sending messages to members.
 */
trait Membership[T] {
  val membership: Membership.Service[Any, T]
}

object Membership {

  trait Service[R, T] {

    /**
     * Get the identity of the current node
     */
    val identity: ZIO[R, Nothing, T]

    /**
     * Send a message to all nodes.
     */
    def broadcast[R1 <: R, A](payload: A)(implicit ev: ByteCodec[R1, A]): ZIO[R1, Error, Unit]

    /**
     * Get a list of all nodes that are currently considered healthy.
     * Note that depending on implementation this might only return the nodes
     * in a local view.
     */
    val nodes: ZIO[R, Nothing, Set[T]]

    /**
     * Send a message to a node.
     */
    def send[A: ByteCodec](to: T, payload: A): ZIO[R, SendError, Unit]

    /**
     * Connect to a remote node, joining the relevant cluster.
     */
    def join(node: T): ZIO[R, Error, Unit]

    /**
     * Send a message to all nodes.
     */
    def broadcast[A: ByteCodec](payload: A): ZIO[R, SendError, Unit]

    /**
     * Send a message to a node.
     */
    def receive[A: ByteCodec]: ZStream[R, Error, A]
  }

}
