package zio.membership.transport

import zio.Has

package object testing {

  type InMemoryTransport[T] = Has[InMemoryTransport.Service[T]]

}
