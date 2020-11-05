package zio.keeper.swim

import zio.ZLayer
import zio.system.System
import zio.config.{ ConfigDescriptor, ReadError, ZConfig }
import zio.config.ConfigDescriptor._
import zio.duration.{ Duration, _ }

case class SwimConfig(
  port: Int,
  protocolInterval: Duration,
  protocolTimeout: Duration,
  messageSizeLimit: Int,
  broadcastResent: Int,
  localHealthMaxMultiplier: Int,
  suspicionAlpha: Int,
  suspicionBeta: Int,
  suspicionRequiredConfirmations: Int
)

object SwimConfig {

  val description: ConfigDescriptor[SwimConfig] =
    (int("PORT").default(5557) |@|
      zioDuration("PROTOCOL_INTERVAL").default(1.second) |@|
      zioDuration("PROTOCOL_TIMEOUT").default(500.milliseconds) |@|
      int("MESSAGE_SIZE_LIMIT").default(64000) |@|
      int("BROADCAST_RESENT").default(10) |@|
      int("LOCAL_HEALTH_MAX_MULTIPLIER").default(8) |@|
      int("SUSPICION_ALPHA_MULTIPLIER").default(9) |@|
      int("SUSPICION_BETA_MULTIPLIER").default(9) |@|
      int("SUSPICION_CONFIRMATIONS").default(3))(SwimConfig.apply, SwimConfig.unapply)

  val fromEnv: ZLayer[System, ReadError[String], ZConfig[SwimConfig]] = ZConfig.fromSystemEnv(description)
}
