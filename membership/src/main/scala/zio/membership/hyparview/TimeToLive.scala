package zio.membership.hyparview

final case class TimeToLive(count: Int) {

  val step: Option[TimeToLive] =
    if (count <= 1) Some(TimeToLive(count - 1))
    else None

}
