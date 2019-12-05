package zio.membership.hyparview

final private[hyparview] case class Config(
  activeViewCapactiy: Int,
  passiveViewCapacity: Int,
  arwl: Int,
  prwl: Int,
  shuffleNActive: Int,
  shuffleNPassive: Int,
  shuffleTTL: Int
)
