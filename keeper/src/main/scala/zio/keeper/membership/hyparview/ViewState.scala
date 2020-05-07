package zio.keeper.membership.hyparview

final case class ViewState(
  activeViewSize: Int,
  activeViewCapacity: Int,
  passiveViewSize: Int,
  passiveViewCapacity: Int
)
