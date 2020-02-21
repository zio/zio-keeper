package zio.keeper.membership.swim

import zio._
import zio.clock.Clock
import zio.duration._
import zio.keeper._
import zio.keeper.discovery.Discovery
import zio.keeper.membership.swim.protocols.Initial.Join
import zio.keeper.membership.swim.protocols.{ DeadLetter, FailureDetection, Initial, User }
import zio.keeper.membership.{ Membership, MembershipEvent, NodeAddress }
import zio.keeper.transport.Transport
import zio.logging.Logging
import zio.logging.slf4j._
import zio.stream.{ Take, ZStream }

object SWIM {

  def run[B: TaggedCodec](
    port: Int
  ): ZManaged[Transport with Discovery with Logging[String] with Clock, Error, Membership[B]] =
    for {
      _   <- logger.info("starting SWIM on port: " + port).toManaged_
      env <- ZManaged.environment[Transport with Discovery]
      messages <- Queue
                   .bounded[Take[Error, (NodeAddress, Chunk[Byte])]](1000)
                   .toManaged(_.shutdown)
      userIn <- Queue
                 .bounded[(NodeAddress, B)](1000)
                 .toManaged(_.shutdown)
      userOut <- Queue
                  .bounded[(NodeAddress, B)](1000)
                  .toManaged(_.shutdown)
      local        <- NodeAddress.local(port).toManaged_
      localAddress <- local.socketAddress.toManaged_
      nodes0       <- Nodes.make(local, messages).toManaged_
      recordedAndInitial <- Initial
                             .protocol(nodes0)
                             .flatMap(_.debug)
                             .flatMap(_.record {
                               case (_, _: Join) => true
                               case _            => false
                             })
                             .toManaged_

      failureDetection <- FailureDetection
                           .protocol(nodes0, 3.seconds)
                           .flatMap(_.piggyBacked(recordedAndInitial).debug)
                           .map(_.binary)
                           .toManaged_
      user <- User
               .protocol[B](userIn, userOut)
               .map(_.binary)
               .toManaged_
      deadLetter <- DeadLetter.protocol.toManaged_
      swim = recordedAndInitial._2.binary
        .compose(failureDetection)
        .compose(user)
        .compose(deadLetter)
      _ <- ZStream
            .fromQueue(messages)
            .collectM {
              case Take.Value((node, msg)) =>
                swim.onMessage(node, msg)
            }
            .collect {
              case Some(msg) => msg
            }
            .merge(swim.produceMessages)
            .mapM(nodes0.send)
            .runDrain
            .toManaged_
            .fork
      _ <- env.transport.bind(localAddress) { conn =>
            NodeAddress(conn.address).flatMap(
              addr =>
                nodes0
                  .accept(addr, conn)
                  .flatMap(
                    cc =>
                      ZStream
                        .repeatEffect(cc.read.map(ch => (addr, ch)))
                        .into(messages)
                  )
                  .ignore
            )
          }
    } yield new Membership[B] {

      override def membership: Membership.Service[Any, B] =
        new Membership.Service[Any, B] {

          override def events: ZStream[Any, Error, MembershipEvent] = ???

          override def localMember: NodeAddress = local

          override def nodes: ZIO[Any, Nothing, List[NodeAddress]] =
            nodes0.currentState.map(_.members.toList)

          override def receive: ZStream[Any, Error, (NodeAddress, B)] =
            ZStream.fromQueue(userIn)

          override def send(data: B, receipt: NodeAddress): ZIO[Any, Error, Unit] =
            userOut.offer((receipt, data)).unit
        }
    }
}
