package zio.keeper.membership.swim

import upickle.default.ReadWriter
import zio.keeper.discovery.Discovery
import zio.keeper.membership.swim.protocols.{ DeadLetter, FailureDetection, Initial, User }
import zio.keeper.transport.Transport
import zio.keeper.{ ByteCodec, TaggedCodec }
import zio.stream.ZStream
import zio.{ Queue, ZManaged }

object SWIM2 {

  def run[A: ReadWriter: ByteCodec, B: TaggedCodec](local: A) =
    for {
      env              <- ZManaged.environment[Transport[A] with Discovery[A]]
      nodes            <- Nodes.make[A](local).toManaged_
      initial          <- Initial.protocol(nodes).toManaged_
      failureDetection <- FailureDetection.protocol(nodes).toManaged_
      userIn           <- Queue.bounded[(A, B)](1000).toManaged(_.awaitShutdown)
      userOut          <- Queue.bounded[(A, B)](1000).toManaged(_.awaitShutdown)
      user             = User.protocol[A, B](userIn, userOut)
      deadLetter       <- DeadLetter.protocol[A].toManaged_
      swim = initial
        .compose(failureDetection)
        .compose(user)
        .compose(deadLetter)
      _ <- env.transport.bind(local) { conn =>
            val cc = new ClusterConnection(conn)
            ZStream
              .repeatEffect(cc.read)
              .mapM(
                msg => swim.onMessage(local, msg.payload)
              )
              .merge(swim.produceMessages.map(Some(_)))
              .collectM {
                case Some((to, payload)) =>
                  nodes.connection(to).flatMap(_.send(payload))
              }
              .runDrain
              .ignore
          }
    } yield ()

}
