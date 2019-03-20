package scalaz.distributed

trait Protocol[Type[_], Path[_, _]] {
  def access[A: Type](id: MetadataID): F[Metadata[Type, Path]]
}
