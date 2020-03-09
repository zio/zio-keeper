package zio.keeper.membership.swim

import zio._
import zio.clock.Clock
import zio.duration._
import zio.keeper._
import zio.keeper.discovery.Discovery
import zio.keeper.membership.swim.protocols._
import zio.keeper.membership.{ Membership, MembershipEvent, NodeAddress }
import zio.keeper.transport.Transport
import zio.logging.Logging
import zio.logging.slf4j._
import zio.stream.ZStream.Pull
import zio.stream.{ Take, ZStream }

object SWIM {

  private def recoverErrors[R, E, A](stream: ZStream[R, E, A]): ZStream[R with Logging[String], E, A] =
    ZStream
      .managed(stream.process)
      .map(
        pull =>
          pull.map(Some(_)).catchAll {
            case Some(e) =>
              logger.error("Error when process stream: " + e) *>
                Pull.emit(None)
            case None => Pull.end
          }
      )
      .flatMap(ZStream.fromPull(_))
      .collect[A] {
        case Some(a) => a
      }

  def run[B: TaggedCodec](
    port: Int
  ): ZManaged[Transport with Discovery with Logging[String] with Clock, Error, Membership[B]] =
    for {
      _   <- logger.info("starting SWIM on port: " + port).toManaged_
      env <- ZManaged.environment[Transport with Discovery]
      messages <- Queue
                   .bounded[Take[Error, Message1]](1000)
                   .toManaged(_.shutdown)
      userIn <- Queue
                 .bounded[(NodeId, B)](1000)
                 .toManaged(_.shutdown)
      userOut <- Queue
                  .bounded[(NodeId, B)](1000)
                  .toManaged(_.shutdown)
      localNodeAddress <- NodeAddress.local(port).toManaged_
      localNodeId      = NodeId.generate

      nodes0 <- Nodes.make(localNodeAddress, localNodeId, messages).toManaged_
      _      <- nodes0.prettyPrint.flatMap(logger.info(_)).repeat(Schedule.spaced(5.seconds)).toManaged_.fork

      initial <- Initial
                  .protocol(nodes0)
                  .flatMap(_.debug)
                  .toManaged_

      failureDetection <- FailureDetection
                           .protocol(nodes0, 3.seconds)
                           .flatMap(_.debug)
                           .map(_.binary)
                           .toManaged_
      suspicion <- Suspicion
                    .protocol(nodes0)
                    .flatMap(_.debug)
                    .map(_.binary)
                    .toManaged_

      user <- User
               .protocol[B](userIn, userOut)
               .map(_.binary)
               .toManaged_
      deadLetter <- DeadLetter.protocol.toManaged_
      swim = initial.binary
        .compose(failureDetection)
        .compose(suspicion)
        .compose(user)
        .compose(deadLetter)
      _ <- ZStream
            .fromQueue(messages)
            .collectM {
              case Take.Value(msg) =>
                swim
                  .onMessage(msg.nodeId, msg.payload)
            }
            .merge(
              recoverErrors(
                swim.produceMessages
              )
            )
            .mapM {
              case direct: Message.Direct[Chunk[Byte]] =>
                nodes0
                  .send(Message1(direct))
                  .catchAll(e => logger.error("error during send: " + e))
              case Message.Empty =>
               ZIO.unit
            }
            .runDrain
            .toManaged_
            .fork

      _ <- localNodeAddress.socketAddress.toManaged_
            .flatMap(
              localAddress =>
                env.transport.bind(localAddress) { conn =>
                  nodes0
                    .accept(conn)
                    .flatMap(_._2.join)
                    .ignore
                }
            )
    } yield new Membership[B] {

      override def membership: Membership.Service[Any, B] =
        new Membership.Service[Any, B] {

          override def events: ZStream[Any, Error, MembershipEvent] =
            nodes0.events

          override def localMember: NodeId = localNodeId

          override def nodes: ZIO[Any, Nothing, List[NodeId]] =
            nodes0.onlyHealthyNodes.map(_.map(_._1))

          override def receive: ZStream[Any, Error, (NodeId, B)] =
            ZStream.fromQueue(userIn)

          override def send(data: B, receipt: NodeId): ZIO[Any, Error, Unit] =
            userOut.offer((receipt, data)).unit
        }
    }
}
