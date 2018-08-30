package net.virtualvoid.fotofinish

import java.io.File
import java.nio.file.Files
import java.text.SimpleDateFormat

import net.virtualvoid.fotofinish.graph.ChineseWhispers
import net.virtualvoid.fotofinish.graph.Graph

import scala.collection.immutable
import scala.runtime.ScalaRunTime

object ClusterTest extends App {
  val Threshold = 0.25 // 0.6*0.6 ?
  val NumFaces = 1000

  val imageFaces: immutable.Seq[(FileInfo, FaceInfo, Int)] =
    MetadataStore.loadAllEntriesFrom(new File("/mnt/hd/fotos/tmp/repo/allmetadata.json.gz"))
      .getEntries[FaceData]
      .filter(_.data.faces.nonEmpty)
      .flatMap { e =>
        val fileInfo = Settings.manager.config.fileInfoOf(e.header.forData)
        e.data.faces.zipWithIndex.map {
          case (info, idx) => (fileInfo, info, idx)
        }
      }
  /*  val imageFaces =
    Settings.manager.allFiles
      .collect {
        case FileAndMetadata(f, m) if m.get[FaceData].exists(_.faces.nonEmpty) =>
          f -> m.get[FaceData].get
      }
      .flatMap {
        case (file, faceData) => faceData.faces.zipWithIndex.map { case (info, idx) => (file, info, idx) }
      }
      .toStream*/

  def sqdist(a1: immutable.Seq[Float], a2: immutable.Seq[Float]): Float =
    (a1, a2).zipped
      .map {
        case (a, b) =>
          val diff = a - b
          diff * diff
      }.sum

  val candidates = imageFaces.take(NumFaces)
  println(s"Found ${candidates.size} candidate faces")

  case class VertexData(repoFile: File, rectangle: Rectangle, faceIndex: Int) {
    override val hashCode: Int = ScalaRunTime._hashCode(this)
  }
  def vertexData(e: (FileInfo, FaceInfo, Int)): VertexData = VertexData(e._1.repoFile, e._2.rectangle, e._3)

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
  val faceDir = new File("/tmp/clusterfaces/")
  faceDir.mkdirs()

  result.toSeq.sortBy(_.size).zipWithIndex.foreach {
    case (cluster, idx) =>
      println()
      println(cluster.map(d => d.repoFile).mkString(" "))

      val dir = new File(s"/tmp/clusters/$idx")
      dir.mkdirs()

      cluster.toSeq.zipWithIndex.foreach(saveFaceToFile(dir))
  }

  def saveFaceToFile(targetDir: File)(e: (VertexData, Int)): Unit = e match {
    case (VertexData(file, rectangle, faceIdx), idx) =>
      import sys.process._
      val meta = Settings.manager.metadataFor(Hash.fromString(HashAlgorithm.Sha512, file.getName))
      val fileDateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss")
      val dateStr = MetadataShortcuts.DateTaken(meta).map(dt => fileDateFormat.format(new java.util.Date(dt.clicks))).getOrElse("unknown")
      val fileName = s"$dateStr-${file.getName}-$faceIdx.jpg"
      val faceFile = new File(faceDir, fileName)

      if (!faceFile.exists) {
        val cmd = s"convert ${file.getAbsolutePath} -crop ${rectangle.width}x${rectangle.height}+${rectangle.left}+${rectangle.top} ${faceFile.getAbsolutePath}"
        println(cmd)
        cmd.!
        faceFile.setWritable(false)
      }
      val targetFile = new File(targetDir, fileName)
      Files.createLink(targetFile.toPath, faceFile.getAbsoluteFile.toPath.toAbsolutePath)
  }

  val unrelatedFaces: Set[VertexData] = (candidates.map(vertexData).toSet diff vertices)
  println(s"Saving ${unrelatedFaces.size} unrelated faces to files")
  val unknownDir = new File(s"/tmp/clusters/unknown")
  unknownDir.mkdirs()
  unrelatedFaces.toSeq.zipWithIndex.foreach(saveFaceToFile(unknownDir))
}