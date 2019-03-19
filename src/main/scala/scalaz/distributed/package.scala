package scalaz

import java.net.InetAddress

import scalaz.zio.IO

package object distributed {
  type F[A] = IO[Error, A]

  val client = new Module {
    override def members(cb: Membership => F[Boolean]): F[Unit] = ???

    override def startup(member: Member, seed: Set[InetAddress]): F[Protocol] = ???
  }
}
