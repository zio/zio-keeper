package zio.keeper.membership.swim

import izumi.reflect.Tags.Tag
import zio._
import zio.clock.Clock
import zio.duration._
import zio.keeper._
import zio.keeper.discovery.Discovery
import zio.keeper.membership.swim.protocols._
import zio.keeper.membership.{ Membership, MembershipEvent, NodeAddress, TaggedCodec }
import zio.logging.Logging.Logging
import zio.logging._
import zio.stream.ZStream

object SWIM {

  def run[B: TaggedCodec: Tag](
    port: Int
  ): ZLayer[Discovery with Logging with Clock, Error, Membership[B]] =
    ZLayer.fromManaged(for {
      _            <- log.info("starting SWIM on port: " + port).toManaged_
      udpTransport <- transport.udp.live(64000).build.map(_.get)

      userIn <- Queue
                 .bounded[Message.Direct[B]](1000)
                 .toManaged(_.shutdown)
      userOut <- Queue
                  .bounded[Message.Direct[B]](1000)
                  .toManaged(_.shutdown)
      localNodeAddress = NodeAddress(Array(0, 0, 0, 0), port)

      nodes0 <- Nodes.make.toManaged_
      _      <- nodes0.prettyPrint.flatMap(log.info(_)).repeat(Schedule.spaced(5.seconds)).toManaged_.fork

      initial <- Initial
                  .protocol(nodes0, localNodeAddress)
                  .flatMap(_.debug)
                  .toManaged_

      failureDetection <- FailureDetection
                           .protocol(nodes0, 3.seconds, 1.second)
                           .flatMap(_.debug)
                           .map(_.binary)
                           .toManaged_
      suspicion <- Suspicion
                    .protocol(nodes0, localNodeAddress, 3.seconds)
                    .flatMap(_.debug)
                    .map(_.binary)
                    .toManaged_

      user <- User
               .protocol[B](userIn, userOut)
               .map(_.binary)
               .toManaged_
      deadLetter <- DeadLetter.protocol.toManaged_
      swim       = Protocol.compose(initial.binary, failureDetection, suspicion, user, deadLetter)
      broadcast0 <- Broadcast.make(64000).toManaged_
      messages0  <- Messages.make(localNodeAddress, broadcast0, udpTransport)
      _          <- messages0.process(swim).toManaged_
    } yield new Membership.Service[B] {

      override def broadcast(data: B): IO[zio.keeper.Error, Unit] =
        for {
          bytes <- TaggedCodec.write[B](data)
          _     <- broadcast0.add(Message.Broadcast(bytes))
        } yield ()

      override val events: ZStream[Any, Error, MembershipEvent] =
        nodes0.events

      override val localMember: NodeAddress = localNodeAddress

      override val nodes: UIO[List[NodeAddress]] =
        nodes0.onlyHealthyNodes.map(_.map(_._1))

      override val receive: ZStream[Any, Error, (NodeAddress, B)] =
        ZStream.fromQueue(userIn).collect {
          case Message.Direct(n, m) => (n, m)
        }

      override def send(data: B, receipt: NodeAddress): IO[Error, Unit] =
        userOut.offer(Message.Direct(receipt, data)).unit

    })
}
