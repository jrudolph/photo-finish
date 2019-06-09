package net.virtualvoid.metadata

trait MetadataStore {
  /**
   * Store a new metadata entry into the repository.
   * @return an updated metadata entry where headers might have been adjusted.
   */
  def push[T](entry: MetadataEntry[T]): MetadataEntry[T]

  /**
   * Allows to keep track of incoming or existing metadata entries of some kind.
   */
  def observe[T](fromSequenceNo: Long, kind: MetadataKindDescriptor.Aux[T])(f: MetadataEntry[T] => Unit): Unit
}
