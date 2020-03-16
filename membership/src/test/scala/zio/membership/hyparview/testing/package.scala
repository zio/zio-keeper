package zio.membership.hyparview

import zio.Has

package object testing {

  type TestPeerService[A] = Has[TestPeerService.Service[A]]

}
