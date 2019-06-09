package net.virtualvoid.metadata

case class MetadataEntry[T](header: MetadataHeader, value: T)