package zio.keeper.membership

import zio.keeper.{ NodeAddress, SendError }
import zio.stream.Stream
import zio.{ IO, UIO }

object Membership {
  /*
   * Project layout:
   *  - zio-keeper-kernel (membership + transport + commons)
   *  - zio-keeper ("full package")
   */

  /*
   * type Keeper = Has[Consensus.Service] with Has[Membership.Service]
   * 
   * object Keeper {
   *   // expose helpers
   * }
   */

  // TODO: re-review Mateusz's PR, and merge it ASAP

  // TODO: drop "debatable" protocols over time; users should not think how to use too much

  /*
   * object Consensus {
   *   type Consensus = Has[Service]
   * 
   *   trait Service {
   *     def sendLeader(..): IO[?, ?]
   *     def leaderEvents: Stream[?, ?]
   *   }
   * 
   *   def live(...): ZLayer[Membership, ?, Consensus] = ???
   * }
   */

  /*
   * trait Service[A] {
   *   def broadcast(data: A): UIO[Unit]
   *   def send(data: A, receipient: NodeAddress): UIO[Unit]
   *   def receive: Stream[Nothing, (NodeAddress, A)]
   * }
   */

  // TODO:
  //  - consider simplifying the interface
  //  - events, message, local member, nodes are debatable
  //  - keep send, broadcast and receive
  trait Service[A] {
    // def invmap[B](f: A => B, g: B => A): Service[B]

    // TODO:
    //  - delay the return or rename to beginBroadcast
    //  - interruption might not be possible 
    def broadcast(data: A): IO[zio.keeper.Error, Unit]

    // TODO:
    // def broadcast(data: A): UIO[Unit]

    def events: Stream[Nothing, MembershipEvent]

    // TODO: rename to self or selfAddress
    def localMember: UIO[NodeAddress]

    // TODO: rename to neighbours
    def nodes: UIO[Set[NodeAddress]]

    // TODO: tuple to case class
    def receive: Stream[Nothing, (NodeAddress, A)]

    // TODO:
    //  - who you can send the message to (direct or via neighbours)
    def send(data: A, receipt: NodeAddress): IO[SendError, Unit]
  }
}
