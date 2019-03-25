package scalaz

import scalaz.zio.IO

package object ziokeeper {
  type Distributed[A] = IO[Error, A]

  val client: Client = Client.default
}
