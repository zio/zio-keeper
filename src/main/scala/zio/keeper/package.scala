package zio

package object keeper {
  type Distributed[A] = IO[Error, A]

  val client: Client = Client.default
}
