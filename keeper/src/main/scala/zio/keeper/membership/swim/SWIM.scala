package zio.keeper.membership.swim

import izumi.reflect.Tags.Tag
import zio.{ IO, Queue, Schedule, UIO, ZIO, ZLayer }
import zio.clock.Clock
import zio.config._
import zio.duration._
import zio.keeper._
import zio.keeper.discovery.Discovery
import zio.keeper.membership.swim.protocols._
import zio.keeper.membership.{ Membership, MembershipEvent }
import zio.logging.Logging
import zio.logging._
import zio.stream._

object SWIM {

  def live[B: TaggedCodec: Tag]
    : ZLayer[Config[SwimConfig] with Discovery with Logging with Clock, Error, Membership[B]] =
    ZLayer.fromManaged(for {
      swimConfig   <- config[SwimConfig].toManaged_
      _            <- log.info("starting SWIM on port: " + swimConfig.port).toManaged_
      udpTransport <- transport.udp.live(swimConfig.messageSizeLimit).build.map(_.get)

      userIn <- Queue
                 .bounded[Message.Direct[B]](1000)
                 .toManaged(_.shutdown)
      userOut <- Queue
                  .bounded[Message.Direct[B]](1000)
                  .toManaged(_.shutdown)
      localNodeAddress <- NodeAddress.local(swimConfig.port).toManaged_

      nodes0 <- Nodes.make.toManaged_
      _      <- nodes0.prettyPrint.flatMap(log.info(_)).repeat(Schedule.spaced(5.seconds)).toManaged_.fork

      initial <- Initial
                  .protocol(nodes0, localNodeAddress)
                  .flatMap(_.debug)
                  .toManaged_

      failureDetection <- FailureDetection
                           .protocol(nodes0, swimConfig.protocolInterval, swimConfig.protocolTimeout)
                           .flatMap(_.debug)
                           .map(_.binary)
                           .toManaged_
      suspicion <- Suspicion
                    .protocol(nodes0, localNodeAddress, swimConfig.suspicionTimeout)
                    .flatMap(_.debug)
                    .map(_.binary)
                    .toManaged_

      user <- User
               .protocol[B](userIn, userOut)
               .map(_.binary)
               .toManaged_
      deadLetter <- DeadLetter.protocol.toManaged_
      swim       = Protocol.compose(initial.binary, failureDetection, suspicion, user, deadLetter)
      broadcast0 <- Broadcast.make(swimConfig.messageSizeLimit, swimConfig.broadcastResent).toManaged_
      messages0  <- Messages.make(localNodeAddress, broadcast0, udpTransport)
      _          <- messages0.process(swim).toManaged_
    } yield new Membership.Service[B] {

      override def broadcast(data: B): IO[zio.keeper.Error, Unit] =
        for {
          bytes <- TaggedCodec.write[User[B]](User(data))
          _     <- broadcast0.add(Message.Broadcast(bytes))
        } yield ()

      override val localMember: UIO[NodeAddress] =
        ZIO.succeed(localNodeAddress)

      override val nodes: UIO[Set[NodeAddress]] =
        nodes0.healthyNodes.map(_.map(_._1).toSet)

      override val receive: Stream[Nothing, (NodeAddress, B)] =
        ZStream.fromQueue(userIn).collect {
          case Message.Direct(n, m) => (n, m)
        }

      override def send(data: B, receipt: NodeAddress): IO[Nothing, Unit] =
        userOut.offer(Message.Direct(receipt, data)).unit

      override val events: Stream[Nothing, MembershipEvent] =
        nodes0.events

    })
}
