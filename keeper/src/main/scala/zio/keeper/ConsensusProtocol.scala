package zio.keeper

import zio._

trait ConsensusProtocol {

  type Consensus = Has[Consensus.Service]

  object Consensus {
    trait Service {}
  }
}
