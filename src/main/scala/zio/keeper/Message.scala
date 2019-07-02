package zio.keeper

final case class Message[S, A](sender: NodeId, state: S, payload: A)
