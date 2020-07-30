package zio.keeper.swim

import zio.{ IO, Queue, Tag, UIO, ZLayer }
import zio.clock.Clock
import zio.config._
import zio.keeper._
import zio.keeper.discovery.Discovery
import zio.keeper.swim.protocols._
import zio.logging.{ Logging, _ }
import zio.stream._
import zio.{ config => _, _ }

object Swim {

  trait Service[A] extends Membership.Service[A] {
    def broadcast(data: A): IO[zio.keeper.Error, Unit]
    def events: Stream[Nothing, MembershipEvent]
    def localMember: NodeAddress
    def nodes: UIO[Set[NodeAddress]]
    def receive: Stream[Nothing, (NodeAddress, A)]
    def send(data: A, receipt: NodeAddress): UIO[Unit]
  }

  type SwimEnv = Config[SwimConfig] with Discovery with Logging with Clock

  final private[this] val QueueSize = 1000

  def live[B: ByteCodec: Tag]: ZLayer[SwimEnv, Error, Swim[B]] = {

    val internalLayer =
      (ZLayer.requires[SwimEnv] ++
        ConversationId.live ++
        Nodes.live ++
        MessageAcknowledge.live) >+> ZLayer.fromManagedMany(
        for {
          swimConfig <- config[SwimConfig].toManaged_
          layer <- (LocalHealthMultiplier.live(swimConfig.localHealthMaxMultiplier) ++
                    SuspicionTimeout.live(
                      swimConfig.protocolInterval,
                      swimConfig.suspicionAlpha,
                      swimConfig.suspicionBeta,
                      swimConfig.suspicionRequiredConfirmations
                    )).build
        } yield Has.allOf(layer.get[SuspicionTimeout.Service], layer.get[LocalHealthMultiplier.Service])
      )

    val managed =
      for {
        env              <- ZManaged.environment[ConversationId with Nodes]
        swimConfig       <- config[SwimConfig].toManaged_
        _                <- log.info("starting SWIM on port: " + swimConfig.port).toManaged_
        udpTransport     <- transport.udp.live(swimConfig.messageSizeLimit).build.map(_.get)
        userIn           <- Queue.bounded[Message.Direct[B]](QueueSize).toManaged(_.shutdown)
        userOut          <- Queue.bounded[Message.Direct[B]](QueueSize).toManaged(_.shutdown)
        localNodeAddress <- NodeAddress.local(swimConfig.port).toManaged_
        initial          <- Initial.protocol(localNodeAddress).flatMap(_.debug).toManaged_
        failureDetection <- FailureDetection
                             .protocol(swimConfig.protocolInterval, swimConfig.protocolTimeout, localNodeAddress)
                             .flatMap(_.debug)
                             .map(_.binary)
                             .toManaged_

        user       <- User.protocol[B](userIn, userOut).map(_.binary).toManaged_
        deadLetter <- DeadLetter.protocol.toManaged_
        swim       = Protocol.compose(initial.binary, failureDetection, user, deadLetter)
        broadcast0 <- Broadcast.make(swimConfig.messageSizeLimit, swimConfig.broadcastResent).toManaged_
        messages0  <- Messages.make(localNodeAddress, broadcast0, udpTransport)
        _          <- messages0.process(swim).toManaged_
      } yield new Swim.Service[B] {

        override def broadcast(data: B): IO[SerializationError, Unit] =
          (for {
            bytes <- ByteCodec.encode[User[B]](User(data))
            _     <- broadcast0.add(Message.Broadcast(bytes))
          } yield ())

        override val receive: Stream[Nothing, (NodeAddress, B)] =
          ZStream.fromQueue(userIn).collect {
            case Message.Direct(n, _, m) => (n, m)
          }

        override def send(data: B, receipt: NodeAddress): UIO[Unit] =
          Message.direct(receipt, data).provide(env).flatMap(userOut.offer(_).unit)

        override def events: Stream[Nothing, MembershipEvent] =
          env.get[Nodes.Service].events

        override def localMember: NodeAddress = localNodeAddress

        override def nodes: UIO[Set[NodeAddress]] =
          env.get[Nodes.Service].healthyNodes.map(_.map(_._1).toSet)
      }

    internalLayer >>> ZLayer.fromManaged(managed)
  }
}
