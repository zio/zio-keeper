package zio.membership.hyparview

import upickle.default._

final case class TimeToLive(count: Int) {

  val step: Option[TimeToLive] =
    if (count <= 1) Some(TimeToLive(count - 1))
    else None

}

object TimeToLive {
  implicit val readWriter: ReadWriter[TimeToLive] = macroRW
}
