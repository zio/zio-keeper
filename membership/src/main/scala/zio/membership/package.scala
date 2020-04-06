package zio

package object membership {
  type Membership[T, A] = Has[Membership.Service[T, A]]
}
