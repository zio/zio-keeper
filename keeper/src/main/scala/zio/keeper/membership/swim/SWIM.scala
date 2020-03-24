package zio.keeper.membership.swim


import izumi.reflect.Tags.Tag
import zio._
import zio.clock.Clock
import zio.duration._
import zio.keeper._
import zio.keeper.discovery.Discovery
import zio.keeper.membership.Membership.Membership
import zio.keeper.membership.swim.protocols._
import zio.keeper.membership.{Membership, MembershipEvent, NodeAddress, TaggedCodec}
import zio.keeper.transport.Transport
import zio.logging.Logging.Logging
import zio.logging._
import zio.stream.{Take, ZStream}

object SWIM {

  private def recoverErrors[R, E, A](stream: ZStream[R, E, A]): ZStream[R with Logging, Nothing, Take[E, A]] =
    ZStream
      .managed(stream.process)
    .mapM(Take.fromPull)
//      .map(
//        pull =>
//          pull.map(Some(_)).catchAll {
//            case Some(e) =>
//              log(LogLevel.Error)("Error when process stream: " + e) *>
//                Pull.emit(None)
//            case None => Pull.end
//          }
//      ).mapM(p => Take.fromPull)
//      .flatMap(ZStream.fromPull)


  def run[B: TaggedCodec: Tag](
    port: Int
  ): ZLayer[Transport with Discovery with Logging with Clock, Error, Membership[B]] =
    ZLayer.fromManaged(
    for {
      _   <- log.info("starting SWIM on port: " + port).toManaged_
//      env <- ZManaged.environment[Transport with Discovery]
      messages <- Queue
                   .bounded[Take[Error, Message.Direct[Chunk[Byte]]]](1000)
                   .toManaged(_.shutdown)
      userIn <- Queue
                 .bounded[Message.Direct[B]](1000)
                 .toManaged(_.shutdown)
      userOut <- Queue
                  .bounded[Message.Direct[B]](1000)
                  .toManaged(_.shutdown)
      localNodeAddress <- NodeAddress.local(port).toManaged_
      localNodeId      = NodeId.generate

      nodes0 <- Nodes.make(localNodeAddress, localNodeId, messages).toManaged_
      _      <- nodes0.prettyPrint.flatMap(log.info(_)).repeat(Schedule.spaced(5.seconds)).toManaged_.fork

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
                  .onMessage(msg)
            }
            .merge(
              recoverErrors(
                swim.produceMessages
              )
            )
            .collectM {
              case Some(msg: Message.Broadcast[Chunk[Byte]]) =>
                ???
              case Some(msg: Message.Direct[Chunk[Byte]]) =>
                nodes0
                  .send(msg)
                  .catchAll(e => log(LogLevel.Error)("error during send: " + e))
            }
            .runDrain
            .toManaged_
            .fork

      _ <- localNodeAddress.socketAddress.toManaged_
            .flatMap(
              localAddress =>
                transport.bind(localAddress) { conn =>
                  nodes0
                    .accept(conn)
                    .flatMap(_._2.join)
                    .ignore
                }
            )
    } yield new Membership.Service[B] {


          override def events: ZStream[Any, Error, MembershipEvent] =
            nodes0.events

          override def localMember: NodeId = localNodeId

          override def nodes: ZIO[Any, Nothing, List[NodeId]] =
            nodes0.onlyHealthyNodes.map(_.map(_._1))

          override def receive: ZStream[Any, Error, (NodeId, B)] =
            ZStream.fromQueue(userIn).collect{
              case Message.Direct(n, m) => (n, m)
            }

          override def send(data: B, receipt: NodeId): ZIO[Any, Error, Unit] =
            userOut.offer(Message.Direct(receipt, data)).unit

    })
}
