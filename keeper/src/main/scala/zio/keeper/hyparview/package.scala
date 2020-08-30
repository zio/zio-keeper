package zio.keeper

import zio._

package object hyparview {
  type HyParViewConfig = Has[HyParViewConfig.Service]
  type PeerService     = Has[PeerService.Service]
  type TRandom         = Has[TRandom.Service]
  type Views           = Has[Views.Service]
}
