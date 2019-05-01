package zio

import scalaz.zio.IO

package object keeper {
  type Distributed[A] = IO[Error, A]

  val client: Client = Client.default
}
