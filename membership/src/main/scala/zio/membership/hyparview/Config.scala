package zio.membership.hyparview

final private[hyparview] case class Config(
  activeViewCapacity: Int,
  passiveViewCapacity: Int,
  arwl: Int,
  prwl: Int,
  shuffleNActive: Int,
  shuffleNPassive: Int,
  shuffleTTL: Int
)
