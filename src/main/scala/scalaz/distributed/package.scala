package scalaz

import scalaz.zio.IO

package object distributed {
  type F[A] = IO[Error, A]
}
