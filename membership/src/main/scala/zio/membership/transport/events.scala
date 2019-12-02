package zio.membership.transport

sealed abstract class Event[T]

final case class Connected[T](
  to: T
) extends Event[T]

final case class Disconnected[T](
  from: T
) extends Event[T]

final case class Bound[T](
  addr: T
) extends Event[T]

final case class Unbound[T](
  addr: T
) extends Event[T]
