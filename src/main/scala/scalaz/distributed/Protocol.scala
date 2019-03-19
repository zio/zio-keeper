package scalaz.distributed

trait Protocol {
  def access[A: Type](id: MetadataID): F[Metadata]
}
