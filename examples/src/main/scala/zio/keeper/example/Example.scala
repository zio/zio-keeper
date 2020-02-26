package zio.keeper.example

import upickle.default._
import zio.clock._
import zio.console._
import zio.duration._
import zio.keeper.TransportError.ExceptionWrapper
import zio.keeper.discovery.Discovery
import zio.keeper.example.TestNode.PingPong.{ Ping, Pong }
import zio.keeper.membership.Membership
import zio.keeper.membership.swim.SWIM
import zio.keeper.transport.Transport
import zio.keeper.{ ByteCodec, TaggedCodec, transport }
import zio.logging.Logging
import zio.logging.slf4j.Slf4jLogger
import zio.macros.delegate._
import zio.macros.delegate.syntax._
import zio.nio.core.{ InetAddress, SocketAddress }
import zio.{ ZIO, ZManaged }

object Node1 extends zio.ManagedApp {

  def run(args: List[String]) =
    TestNode.start(5557, Set.empty)
}

object Node2 extends zio.ManagedApp {

  def run(args: List[String]) =
    TestNode.start(5558, Set(5559, 5557))
}

object Node3 extends zio.ManagedApp {

  def run(args: List[String]) =
    TestNode.start(5559, Set(5558))
}

object TestNode {

  val loggingEnv = ZIO.environment[zio.ZEnv] @@ enrichWith[Logging[String]](
    new Slf4jLogger.Live {

      override def formatMessage(msg: String): ZIO[Any, Nothing, String] =
        ZIO.succeed(msg)
    }
  )

  val tcp =
    loggingEnv >>>
      ZIO.environment[zio.ZEnv with Logging[String]] @@
        transport.tcp.withTcpTransport(10.seconds, 10.seconds)

  sealed trait PingPong

  object PingPong {
    case class Ping(i: Int) extends PingPong
    case class Pong(i: Int) extends PingPong

    implicit val pingCodec: ByteCodec[Ping] =
      ByteCodec.fromReadWriter(macroRW[Ping])

    implicit val pongCodec: ByteCodec[Pong] =
      ByteCodec.fromReadWriter(macroRW[Pong])

    implicit def tagged(implicit p1: ByteCodec[Ping], p2: ByteCodec[Pong]) =
      TaggedCodec.instance[PingPong]({
        case Ping(_) => 1
        case Pong(_) => 2
      }, {
        case 1 => p1.asInstanceOf[ByteCodec[PingPong]]
        case 2 => p2.asInstanceOf[ByteCodec[PingPong]]
      })
  }

  def start(port: Int, otherPorts: Set[Int]) =
    (environment(port, otherPorts) >>> (for {
      env   <- ZManaged.environment[Membership[PingPong]]
      _     <- sleep(5.seconds).toManaged_
      nodes <- env.membership.nodes.toManaged_
      _     <- ZIO.foreach(nodes)(n => env.membership.send(Ping(1), n)).toManaged_
      _ <- env.membership.receive.foreach {
            case (sender, message) =>
              putStrLn("receive message: " + message) *> env.membership.send(Pong(1), sender).ignore *> sleep(5.seconds)
          }.toManaged_
    } yield ()))
      .fold(ex => {
        println(s"exit with error: $ex")
        1
      }, _ => 0)

  private def environment(port: Int, others: Set[Int]) =
    discoveryEnv(others).toManaged_ @@
      enrichWithManaged(tcp.toManaged_) >>>
      membership(port)

  def discoveryEnv(others: Set[Int]) =
    ZIO.environment[zio.ZEnv] @@
      enrichWithM[Discovery](
        ZIO
          .foreach(others)(
            port => InetAddress.localHost.flatMap(addr => SocketAddress.inetSocketAddress(addr, port))
          )
          .mapError(ExceptionWrapper(_))
          .map(addrs => Discovery.staticList(addrs.toSet))
      )

  def membership(port: Int) =
    ZManaged.environment[zio.ZEnv with Logging[String] with Transport with Discovery] @@
      enrichWithManaged(
        SWIM.run[PingPong](port)
      )
}
