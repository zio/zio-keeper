package zio.keeper.membership.swim

import zio.config.ConfigDescriptor._
import zio.duration.{ Duration, _ }

case class SwimConfig(
  port: Int,
  protocolInterval: Duration,
  protocolTimeout: Duration,
  suspicionTimeout: Duration,
  messageSizeLimit: Int,
  broadcastResent: Int
)

object SwimConfig {

  val description =
    (int("PORT").default(55557) |@|
      zioDuration("PROTOCOL_INTERVAL").default(3.seconds) |@|
      zioDuration("PROTOCOL_TIMEOUT").default(1.seconds) |@|
      zioDuration("SUSPICION_TIMEOUT").default(3.seconds) |@|
      int("MESSAGE_SIZE_LIMIT").default(64000) |@|
      int("BROADCAST_RESENT").default(10))(SwimConfig.apply, SwimConfig.unapply)
}
