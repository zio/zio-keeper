package scalaz

package object distributed {

  val client = new Client {
    type Type[A] = SupportedType[A]

    override def key[K: Type, V: Type](k: K): Path[Map[K, V], V] = Path.Key[K, V](k)

    override def compose[A: Type, B: Type, C: Type](x: Path[A, B], y: Path[B, C]): Path[A, C] =
      x >>> y

    override def connect(member: Member, seed: Set[InetAddress]): Distributed[Protocol] = ???

    override def members(cb: Membership => Distributed[Boolean]): Distributed[Unit] = ???
  }
}
