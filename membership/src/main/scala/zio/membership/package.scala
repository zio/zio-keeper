package zio

package object membership {
  type Membership[T] = Has[Membership.Service[T]]
}
