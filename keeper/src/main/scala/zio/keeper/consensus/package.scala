package zio.keeper

import zio.Has

package object consensus {

  type Consensus = Has[Consensus.Service]

  type ReceiverAdapter = Has[ReceiverAdapter.Service]

  type JGroups = Has[JGroups.Service]

}
