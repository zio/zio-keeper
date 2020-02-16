package zio.keeper.membership.swim

import upickle.default.ReadWriter
import zio._
import zio.clock.Clock
import zio.keeper.discovery.Discovery
import zio.keeper.membership.swim.protocols.{DeadLetter, FailureDetection, Initial, User}
import zio.keeper.membership.{Membership, MembershipEvent}
import zio.keeper.transport.Transport
import zio.keeper.{ByteCodec, TaggedCodec}
import zio.logging.Logging
import zio.stream.ZStream

object SWIM2 {

  def run[A: ReadWriter: ByteCodec, B: TaggedCodec](local: A):
  ZManaged[Transport[A] with Discovery[A] with Logging[String] with Clock, zio.keeper.Error, Membership.Service[Any, A, B]] =
    for {
      env              <- ZManaged.environment[Transport[A] with Discovery[A]]
      nodes0            <- Nodes.make[A](local).toManaged_
      initial          <- Initial.protocol(nodes0).toManaged_
      failureDetection <- FailureDetection.protocol(nodes0).toManaged_
      userIn           <- Queue.bounded[(A, B)](1000).toManaged(_.awaitShutdown)
      userOut          <- Queue.bounded[(A, B)](1000).toManaged(_.awaitShutdown)
      user             <- User.protocol[A, B](userIn, userOut).toManaged_
      deadLetter       <- DeadLetter.protocol[A].toManaged_
      swim = initial
        .compose(failureDetection)
        .compose(user)
        .compose(deadLetter)
      _ <- env.transport.bind(local) { conn =>
            val cc = new ClusterConnection(conn)
            nodes0.accept(cc)
            ZStream
              .repeatEffect(cc.read)
              .mapM(
                msg => swim.onMessage(local, msg.payload)
              )
              .merge(swim.produceMessages.map(Some(_)))
              .collectM {
                case Some((to, payload)) =>
                  nodes0.connection(to).flatMap(_.send(payload))
              }
              //.catchAll()
              .runDrain
              .ignore
          }
    } yield new Membership.Service[Any, A, B] {

      override def events: ZStream[Any, keeper.Error, MembershipEvent[A]] = ???

      override def localMember: A = local

      override def nodes: ZIO[Any, Nothing, List[A]] =
        nodes0.currentState.map(_.members.toList)

      override def receive: ZStream[Any, keeper.Error, (A, B)] =
        ZStream.fromQueue(userIn)

      override def send(data: B, receipt: A): ZIO[Any, keeper.Error, Unit] =
        userOut.offer((receipt, data)).unit
    }

}
