package zio.keeper.transport

import zio.Has

package object testing {

  type InMemoryTransport = Has[InMemoryTransport.Service]

}
