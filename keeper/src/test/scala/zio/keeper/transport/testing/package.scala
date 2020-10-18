package zio.keeper.transport

import zio.Has

package object testing {

  type TestTransport = Has[TestTransport.Service]

}
