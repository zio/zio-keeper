package zio.keeper.swim

import zio.Layer
import zio.config.{ Config, ConfigDescriptor, ReadError }
import zio.config.ConfigDescriptor._
import zio.duration.{ Duration, _ }

case class SwimConfig(
  port: Int,
  protocolInterval: Duration,
  protocolTimeout: Duration,
  suspicionTimeout: Duration,
  messageSizeLimit: Int,
  broadcastResent: Int,
  localHealthMaxMultiplier: Int
)

object SwimConfig {

  val description: ConfigDescriptor[SwimConfig] =
    (int("PORT").default(5557) |@|
      zioDuration("PROTOCOL_INTERVAL").default(3.seconds) |@|
      zioDuration("PROTOCOL_TIMEOUT").default(1.seconds) |@|
      zioDuration("SUSPICION_TIMEOUT").default(3.seconds) |@|
      int("MESSAGE_SIZE_LIMIT").default(64000) |@|
      int("BROADCAST_RESENT").default(10) |@|
      int("LOCAL_HEALTH_MAX_MULTIPLIER").default(9))(SwimConfig.apply, SwimConfig.unapply)

  val fromEnv: Layer[ReadError[String], Config[SwimConfig]] = Config.fromSystemEnv(description)
}
