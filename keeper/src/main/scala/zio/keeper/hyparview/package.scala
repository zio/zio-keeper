package zio.keeper

import zio._

package object hyparview {
  type RawMessage      = Chunk[Byte]
  type PeerService     = Has[PeerService.Service]
  type HyParViewConfig = Has[HyParViewConfig.Service]
  type TRandom         = Has[TRandom.Service]
  type Views           = Has[Views.Service]

  type Enqueue[-A] = ZQueue[Any, Any, Nothing, Nothing, A, Any]

}
