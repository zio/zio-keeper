package zio

package object membership {
  type Membership[T, A] = Has[Membership.Service[T, A]]
  type PeerService[T]   = Has[PeerService.Service[T]]
}
