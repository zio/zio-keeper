package zio.membership

import zio._
import zio.stream._
import zio.keeper.membership.ByteCodec

object Membership {

  /**
   * The main entrypoint to the membership library. Allows querying the cluster state
   * and sending messages to members.
   */
  trait Service[T] {

    /**
     * Get the identity of the current node
     */
    val identity: IO[Nothing, T]

    /**
     * Get a list of all nodes that are currently considered healthy.
     * Note that depending on implementation this might only return the nodes
     * in a local view.
     */
    val nodes: IO[Nothing, Set[T]]

    /**
     * Send a message to a node.
     */
    def send[A: ByteCodec](to: T, payload: A): IO[SendError, Unit]

    /**
     * Connect to a remote node, joining the relevant cluster.
     */
    def join(node: T): IO[Error, Unit]

    /**
     * Send a message to all nodes.
     */
    def broadcast[A: ByteCodec](payload: A): IO[SendError, Unit]

    /**
     * Send a message to a node.
     */
    def receive[A: ByteCodec]: Stream[Error, A]
  }
}
