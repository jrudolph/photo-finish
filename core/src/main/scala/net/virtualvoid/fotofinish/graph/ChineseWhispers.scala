package net.virtualvoid.fotofinish.graph

import java.util.concurrent.ThreadLocalRandom

import scala.annotation.tailrec
import scala.collection.immutable

object ChineseWhispers {
  def cluster[V](graph: Graph[V], iterations: Int = 50): immutable.Seq[Set[V]] = {
    type Labels = Map[V, Int]
    val initialLabels = graph.vertices.iterator.zip(Iterator.from(1)).toMap
    val vertices = graph.vertices.toVector
    val outgoingVertices: Map[V, immutable.Set[V]] =
      graph.edges.flatMap {
        case (a, b) => (a, b) :: (b, a) :: Nil
      }.groupBy(_._1).map {
        case (origin, outgoing) => origin -> outgoing.map(_._2).toSet
      }.toMap
    def bestCandidate(labels: Labels, from: V): Int =
      outgoingVertices(from).toVector
        .map(labels)
        .groupBy(identity)
        .toVector
        .sortBy(-_._2.size)
        .head.
        _1
    val random = ThreadLocalRandom.current()

    @tailrec
    def step(labels: Labels, remainingSteps: Int): Labels =
      if (remainingSteps == 0) labels
      else {
        @tailrec
        def substep(toVisit: immutable.Seq[V], labels: Labels): Labels =
          if (toVisit.isEmpty) labels
          else {
            val chosenIdx = random.nextInt(toVisit.size)
            val chosen = toVisit(chosenIdx)
            val remaining = toVisit.take(chosenIdx - 1) ++ toVisit.takeRight(toVisit.size - 1 - chosenIdx)
            val newLabels = labels + (chosen -> bestCandidate(labels, chosen))
            substep(remaining, newLabels)
          }

        val newLabels = substep(vertices, labels)
        step(newLabels, remainingSteps - 1)
      }

    step(initialLabels, iterations).toVector.groupBy(_._2).map {
      case (groupId, els) => els.map(_._1).toSet
    }.toVector
  }
}
