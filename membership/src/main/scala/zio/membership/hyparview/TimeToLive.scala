package zio.membership.hyparview

import upickle.default._

final private[hyparview] case class TimeToLive(count: Int) {

  val step: Option[TimeToLive] =
    if (count <= 1) Some(TimeToLive(count - 1))
    else None

}

private[hyparview] object TimeToLive {
  implicit val readWriter: ReadWriter[TimeToLive] = macroRW
}
