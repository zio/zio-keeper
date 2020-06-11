package zio.keeper.hyparview

final case class ViewState(
  activeViewSize: Int,
  activeViewCapacity: Int,
  passiveViewSize: Int,
  passiveViewCapacity: Int
)
