package zio.keeper.example

import upickle.default._
import zio._
import zio.clock._
import zio.config.Config
import zio.duration._
import zio.keeper.ByteCodec
import zio.keeper.discovery.Discovery
import zio.keeper.ByteCodec
import zio.keeper.swim.{ Swim, SwimConfig }
import zio.logging.Logging
import zio.nio.core.InetAddress
import zio.keeper.swim._
import zio.logging._
import zio.console._
import zio.keeper.example.K8sTestNode.ChaosMonkey.SimulateCpuSpike

object K8sTestNode extends zio.App {

  val discovery =
    ZLayer.fromManaged(
      for {
        config <- ZManaged.environment[Config[SwimConfig]].map(_.get)
        serviceDns <- InetAddress
                       .byName("zio-keeper-node.zio-keeper-experiment.svc.cluster.local")
                       .orDie
                       .toManaged_
        discovery <- Discovery.k8Dns(serviceDns, 10.seconds, config.port).build.map(_.get)
      } yield discovery
    )

  val dependencies = {
    val config     = SwimConfig.fromEnv.orDie
    val logging    = Logging.console((_, msg) => msg)
    val seeds      = (logging ++ config) >>> discovery
    val membership = (seeds ++ logging ++ Clock.live ++ config) >>> Swim.live[ChaosMonkey]
    logging ++ membership
  }

  def run(args: List[String]) =
    program
      .provideCustomLayer(dependencies)
      .catchAll(ex => putStrLn("error: " + ex).as(1))

  sealed trait ChaosMonkey

  object ChaosMonkey {
    final case object SimulateCpuSpike extends ChaosMonkey

    implicit val cpuSpikeCodec: ByteCodec[SimulateCpuSpike.type] =
      ByteCodec.fromReadWriter(macroRW[SimulateCpuSpike.type])

    implicit val codec: ByteCodec[ChaosMonkey] =
      ByteCodec.tagged[ChaosMonkey][
        SimulateCpuSpike.type
      ]
  }

  val program =
//   Fiber.dumpAll.flatMap(ZIO.foreach(_)(_.prettyPrintM.flatMap(putStrLn(_).provideLayer(ZEnv.live)))).delay(10.seconds).uninterruptible.fork.toManaged_ *>
    receive[ChaosMonkey]
      .foreach {
        case (sender, message) =>
          log.info(s"receive message: $message from: $sender") *>
            ZIO.whenCase(message) {
              case SimulateCpuSpike => log.info("simulating cpu spike")
            }
      }
      .as(0)

}
