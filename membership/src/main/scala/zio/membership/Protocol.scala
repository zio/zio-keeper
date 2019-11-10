package zio.membership

// TODO: do we want a seperate sealed trait per subprotocol?
sealed trait Protocol
object Protocol {
  // Swim Protocol - Probes
  final case class Ping() extends Protocol
  final case class PingReq() extends Protocol
  case object Ack extends Protocol
  case object NAck extends Protocol

  // Swim Protocol - Suspicion
  final case class Suspect() extends Protocol
  final case class Alive() extends Protocol
  final case class Dead() extends Protocol

  // Swim Protocol - Joinining and Leaving
  // TODO: Do we want to do this here or at a higher level like memberlist/serf?
  final case class Joined() extends Protocol
  final case class Left() extends Protocol

  // Special message to piggyback other messages
  final case class Compound() extends Protocol
}
