package zio.membership

import zio._
import zio.stream._

import zio.keeper.membership.ByteCodec

object Membership {

  /**
   * The main entrypoint to the membership library. Allows querying the cluster state
   * and sending messages to members.
   */
  trait Service[T, A] {

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
    def send(to: T, payload: A)(implicit ev: ByteCodec[A]): IO[SendError, Unit]

    /**
     * Send a message to all nodes.
     */
    def broadcast(payload: A)(implicit ev: ByteCodec[A]): IO[SendError, Unit]

    /**
     * Send a message to a node.
     */
    def receive(implicit ev: ByteCodec[A]): Stream[Nothing, A]
  }
}
