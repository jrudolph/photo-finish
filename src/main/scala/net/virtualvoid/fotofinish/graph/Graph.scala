package net.virtualvoid.fotofinish.graph

import scala.collection.immutable

trait Graph[V] {
  def vertices: immutable.Set[V]
  def edges: immutable.Seq[(V, V)]
}
object Graph {
  def apply[V](
    vertices: immutable.Set[V],
    edges:    immutable.Seq[(V, V)]): Graph[V] =
    GraphImpl(vertices, edges)

  private case class GraphImpl[V](
      vertices: immutable.Set[V],
      edges:    immutable.Seq[(V, V)]) extends Graph[V]
}
