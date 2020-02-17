package zio.keeper.membership.swim

import zio.clock.Clock
import zio.keeper.TaggedCodec
import zio.keeper.discovery.Discovery
import zio.keeper.membership.swim.protocols.{ DeadLetter, FailureDetection, Initial, User }
import zio.keeper.membership.{ Membership, MembershipEvent, NodeAddress }
import zio.keeper.transport.Transport
import zio.logging.Logging
import zio.logging.slf4j._
import zio.stream.{ Take, ZStream }
import zio.{ keeper, _ }
import zio.duration._

object SWIM2 {

  def run[B: TaggedCodec](
    port: Int
  ): ZManaged[Transport with Discovery with Logging[String] with Clock, zio.keeper.Error, Membership[
    NodeAddress,
    B
  ]] =
    for {
      _                <- logger.info("starting SWIM on port: " + port).toManaged_
      env              <- ZManaged.environment[Transport with Discovery]
      messages         <- Queue.bounded[Take[keeper.Error, (NodeAddress, Chunk[Byte])]](1000).toManaged(_.awaitShutdown)
      local            <- NodeAddress.local(port).toManaged_
      localAddress     <- local.socketAddress.toManaged_
      nodes0           <- Nodes.make(local, messages).toManaged_
      initial          <- Initial.protocol(nodes0).toManaged_
      failureDetection <- FailureDetection.protocol(nodes0, 3.seconds).toManaged_
      userIn           <- Queue.bounded[(NodeAddress, B)](1000).toManaged(_.awaitShutdown)
      userOut          <- Queue.bounded[(NodeAddress, B)](1000).toManaged(_.awaitShutdown)
      user             <- User.protocol[B](userIn, userOut).toManaged_
      deadLetter       <- DeadLetter.protocol.toManaged_
      swim = initial
        .compose(failureDetection)
        .compose(user)
        .compose(deadLetter)
      _ <- swim.produceMessages
            .mapM {
              case (to, payload) =>
                nodes0
                  .connection(to)
                  .flatMap(_.send(payload))
            }
            .runDrain
            .toManaged_
            .fork
      _ <- ZStream
            .fromQueue(messages)
          .tap(x => logger.info(x.toString))
            .mapM {
              case Take.Value((node, msg)) =>
                swim.onMessage(node, msg)
              case _ => ZIO.succeed(None)
            }
            .collectM {
              case Some((to, payload)) =>
                nodes0
                  .connection(to)
                  .flatMap(_.send(payload))
            }
            //.catchAll()
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
                        .repeatEffect(cc.read)
                        .mapM(
                          msg => swim.onMessage(addr, msg)
                        )
                        .collectM {
                          case Some((to, payload)) =>
                            nodes0
                              .connection(to)
                              .flatMap(_.send(payload))
                        }
                        //.catchAll()
                        .runDrain
                  )
                  .ignore
            )
          }
    } yield new Membership[NodeAddress, B] {

      override def membership: Membership.Service[Any, NodeAddress, B] =
        new Membership.Service[Any, NodeAddress, B] {

          override def events: ZStream[Any, keeper.Error, MembershipEvent[NodeAddress]] = ???

          override def localMember: NodeAddress = local

          override def nodes: ZIO[Any, Nothing, List[NodeAddress]] =
            nodes0.currentState.map(_.members.toList)

          override def receive: ZStream[Any, keeper.Error, (NodeAddress, B)] =
            ZStream.fromQueue(userIn)

          override def send(data: B, receipt: NodeAddress): ZIO[Any, keeper.Error, Unit] =
            userOut.offer((receipt, data)).unit
        }
    }
}
