package zio.keeper.example

import upickle.default._
import zio._
import zio.clock._
import zio.config.Config
import zio.console._
import zio.duration._
import zio.keeper.ByteCodec
import zio.keeper.discovery.Discovery
import zio.keeper.example.TestNode.PingPong.{ Ping, Pong }
import zio.keeper.TaggedCodec
import zio.keeper.membership.swim.{ SWIM, SwimConfig }
import zio.logging.Logging
import zio.nio.core.{ InetAddress, SocketAddress }
import zio.keeper.membership._
import zio.logging._

object Node1 extends zio.App {

  def run(args: List[String]) =
    TestNode.start(5557, Set.empty)
}

object Node2 extends zio.App {

  def run(args: List[String]) =
    TestNode.start(5558, Set(5557))
}

object Node3 extends zio.App {

  def run(args: List[String]) =
    TestNode.start(5559, Set(5557))
}

object TestNode {

  val logging = Logging.console((_, msg) => msg)

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
//   Fiber.dumpAll.flatMap(ZIO.foreach(_)(_.prettyPrintM.flatMap(putStrLn(_).provideLayer(ZEnv.live)))).delay(10.seconds).uninterruptible.fork.toManaged_ *>
    (for {
      _ <- sleep(5.seconds)
      _ <- events[PingPong].foreach(event => log.info("membership event: " + event)).fork
      _ <- broadcast[PingPong](Ping(1))
      _ <- receive[PingPong].foreach {
            case (sender, message) =>
              log.info(s"receive message: $message from: $sender") *>
                ZIO.whenCase(message) {
                  case Ping(i) => send[PingPong](Pong(i + 1), sender).ignore
                  case Pong(i) => send[PingPong](Pong(i + 1), sender).ignore
                } *> sleep(5.seconds)
          }
    } yield 0)
      .provideCustomLayer(environment(port, otherPorts))
      .catchAll(ex => putStrLn("error: " + ex).as(1))

  private def environment(port: Int, others: Set[Int]) = {
    val config     = Config.fromMap(Map("PORT" -> port.toString), SwimConfig.description).orDie
    val seeds      = discovery(others)
    val membership = (seeds ++ logging ++ Clock.live ++ config) >>> SWIM.live[PingPong]
    logging ++ membership
  }

  def discovery(others: Set[Int]): ULayer[Discovery] =
    ZLayer.fromManaged(
      ZManaged
        .foreach(others) { port =>
          InetAddress.localHost.flatMap(SocketAddress.inetSocketAddress(_, port)).toManaged_
        }
        .orDie
        .flatMap(addrs => Discovery.staticList(addrs.toSet).build.map(_.get))
    )
}
