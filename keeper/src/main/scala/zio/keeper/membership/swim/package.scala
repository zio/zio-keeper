package zio.keeper.membership

import zio.Has

package object swim {

  type SWIM[B] = Has[SWIM.Service[B]]

}
