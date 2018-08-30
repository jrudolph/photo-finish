package net.virtualvoid.fotofinish

import java.io.File

import net.virtualvoid.fotofinish.graph.ChineseWhispers
import net.virtualvoid.fotofinish.graph.Graph

import scala.collection.immutable

object ClusterTest extends App {
  val Threshold = 0.25 // 0.6*0.6 ?
  val NumFaces = 200

  val imageFaces =
    Settings.manager.allFiles
      .collect {
        case FileAndMetadata(f, m) if m.get[FaceData].exists(_.faces.nonEmpty) =>
          f -> m.get[FaceData].get
      }
      .flatMap {
        case (file, faceData) => faceData.faces.map(file ->)
      }
      .toStream

  def sqdist(a1: immutable.Seq[Float], a2: immutable.Seq[Float]): Float =
    (a1, a2).zipped
      .map {
        case (a, b) =>
          val diff = a - b
          diff * diff
      }.sum

  val candidates = imageFaces.take(NumFaces)
  println(s"Found ${candidates.size} candidate faces")

  type VertexData = (File, Rectangle)
  def vertexData(e: (FileInfo, FaceInfo)): VertexData = (e._1.repoFile, e._2.rectangle)

  val edges =
    candidates.flatMap { c1 =>
      candidates
        .filter(_._1.repoFile.getName.compareTo(c1._1.repoFile.getName) > 0) // edges just in one direction
        .filter(c2 => sqdist(c1._2.modelData, c2._2.modelData) < Threshold)
        .map(c2 => vertexData(c1) -> vertexData(c2))
    }

  val vertices: Set[VertexData] = edges.flatMap(e => e._1 :: e._2 :: Nil).toSet
  println(s"Found ${edges.size} edges in ${vertices.size} vertices")
  val graph = Graph(vertices, edges)
  val result = ChineseWhispers.cluster(graph, iterations = 20)
  println(s"Found ${result.size} clusters")

  result.toSeq.zipWithIndex.foreach {
    case (cluster, idx) =>
      println()
      println(cluster.map(d => d._1).mkString(" "))

      val dir = new File(s"/tmp/clusters/$idx")
      dir.mkdirs()

      cluster.toSeq.zipWithIndex.foreach(saveFaceToFile(dir))
  }
  def saveFaceToFile(dir: File)(e: ((File, Rectangle), Int)): Unit = e match {
    case ((file, rectangle), idx) =>
      import sys.process._
      val fileName = new File(dir, s"$idx.jpg")
      val cmd = s"convert ${file.getAbsolutePath} -crop ${rectangle.width}x${rectangle.height}+${rectangle.left}+${rectangle.top} ${fileName.getAbsolutePath}"
      cmd.!
  }

  val unrelatedFaces: Set[VertexData] = (candidates.map(vertexData).toSet diff vertices)
  println(s"Saving ${unrelatedFaces.size} unrelated faces to files")
  val unknownDir = new File(s"/tmp/clusters/unknown")
  unknownDir.mkdirs()
  unrelatedFaces.toSeq.zipWithIndex.foreach(saveFaceToFile(unknownDir))
}