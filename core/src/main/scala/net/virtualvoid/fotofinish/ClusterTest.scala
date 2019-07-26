package net.virtualvoid.fotofinish

import metadata._
import java.io.File
import java.nio.file.Files
import java.text.SimpleDateFormat

import net.virtualvoid.fotofinish.graph.ChineseWhispers
import net.virtualvoid.fotofinish.graph.Graph

import scala.collection.immutable
import scala.io.Source

/*
TODO:
 * allow multiple mappings per person
 * detect mapping conflicts
 */

object ClusterTest extends App {
  val Threshold = 0.2 // 0.6*0.6 ?
  val NumFaces = 20000
  val ShortHash = 20

  type FeatureVector = Array[Float]

  println("Loading mappings...")
  val Mappings: Seq[(String, String)] =
    Source.fromFile("mappings.txt").getLines().map { l =>
      val Array(id, name) = l.split("->")
      id.trim -> name.trim
    }.toSeq

  println("Loading faces...")
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

  def sqdistFloat(a1: FeatureVector, a2: FeatureVector): Float = {
    /* naive but slow because of boxing algorithm
    (a1, a2).zipped
      .map {
        case (a, b) =>
          val diff = a - b
          diff * diff
      }.sum
     */

    var i = 0
    var sqDiff = 0f
    while (i < a1.length && sqDiff < Threshold) {
      val diff = a1(i) - a2(i)
      sqDiff += diff * diff

      i += 1
    }
    sqDiff
  }

  def centroidFloat(points: immutable.Seq[FeatureVector]): FeatureVector =
    points.transpose.map(cn => cn.sum / cn.size.toFloat).toArray

  implicit class FaceInfoOps(val one: FaceInfo) extends AnyVal {
    def sqdist(other: FaceInfo): Float = sqdistFloat(one.modelData, other.modelData)
  }

  val candidates = imageFaces.take(NumFaces)
  println(s"Found ${candidates.size} candidate faces")

  case class VertexData(repoFile: File, rectangle: Rectangle, faceIndex: Int, faceInfo: FaceInfo) {
    val id = s"${repoFile.getName}-$faceIndex"
    val shortId = s"${repoFile.getName.take(ShortHash)}-$faceIndex"

    override val hashCode: Int = shortId.hashCode
    override def equals(obj: scala.Any): Boolean = obj match {
      case vd: VertexData => shortId == vd.shortId
      case _              => super.equals(obj)
    }
  }
  def vertexData(e: (FileInfo, FaceInfo, Int)): VertexData = VertexData(e._1.repoFile, e._2.rectangle, e._3, e._2)

  val edges =
    candidates
      .par
      .flatMap { c1 =>
        candidates
          .filter { c2 =>
            c2._1.repoFile.getName.compareTo(c1._1.repoFile.getName) > 0 &&
              c1._2.sqdist(c2._2) < Threshold
          }
          .map(c2 => vertexData(c1) -> vertexData(c2))
      }
      .seq

  val vertices: Set[VertexData] = edges.flatMap(e => e._1 :: e._2 :: Nil).toSet
  println(s"Found ${edges.size} edges in ${vertices.size} vertices")
  println("Now clustering")
  val graph = Graph(vertices, edges)
  val result = ChineseWhispers.cluster(graph, iterations = 20)
  println(s"Found ${result.size} clusters")
  val faceDir = new File("/tmp/clusterfaces/")
  faceDir.mkdirs()

  println("Calculating uncanny reappearances")
  val uncannyReappearances =
    result.toSeq
      .sortBy(_.size)
      .foreach { set =>
        val withDates =
          set.flatMap { vd =>
            val meta = Settings.manager.metadataFor(Hash.fromString(HashAlgorithm.Sha512, vd.repoFile.getName))
            MetadataShortcuts.DateTaken(meta).map(dt => vd -> dt)
          }

        if (withDates.size > 1) {
          val min = withDates.minBy(_._2)
          val max = withDates.maxBy(_._2)

          if ((max._2.clicks - min._2.clicks) > 1000L * 3600 * 24 * 365 * 5)
            println(s"Person reappeared after long time ${min._1.shortId} ${max._1.shortId} ${min._2} ${max._2} total ${set.size}")
        }
      }

  println("Preparing results")
  result.toSeq
    // uncomment to remove single images .filter(_.size > 2)
    .sortBy(_.size).zipWithIndex.foreach {
      case (cluster, idx) =>
        println()
        //println(cluster.map(d => d.repoFile).mkString(" "))

        val clusterName =
          Mappings.find(m => cluster.exists { vd => m._1 == vd.id || m._1 == vd.shortId })
            .map(_._2)
            .getOrElse(idx.toString)

        val features = cluster.toVector.map(_.faceInfo.modelData)
        val centroid = centroidFloat(features)
        val distsToCentroid = features.map(other => sqdistFloat(centroid, other))
        val maxSqDistToCentroid = distsToCentroid.max
        val minSqDistToCentroid = distsToCentroid.min

        val allSumDists =
          cluster.map { c1 =>
            val distSum =
              cluster
                .filterNot(_ == c1)
                .map(c2 => math.sqrt(c1.faceInfo.sqdist(c2.faceInfo)))
                .sum
            (c1 -> distSum)
          }
        val medoid = allSumDists.minBy(_._2)._1
        val distsToMedoid = features.map(other => sqdistFloat(medoid.faceInfo.modelData, other))
        val maxSqDistToMedoid = distsToMedoid.max
        val minSqDistToMedoid = distsToMedoid.min

        println(s"Found ${cluster.size} images of [$clusterName]")
        println(s"maxSqDistToCentroid [$maxSqDistToCentroid] minSqDistToCentroid [$minSqDistToCentroid]")
        println(s"maxSqDistToMedoid [$maxSqDistToMedoid] minSqDistToMedoid [$minSqDistToMedoid] medoid: ${medoid.shortId}")

        val clusterDir = new File(s"/tmp/clusters/$clusterName")
        val clusterDirByDate = new File(s"/tmp/clusters/$clusterName/by-date")
        clusterDirByDate.mkdirs()

        cluster.toSeq.foreach { vd =>
          val fileName = fileNameForVertexData(vd)
          val faceFile = ensureFace(vd)
          val dist = sqdistFloat(medoid.faceInfo.modelData, vd.faceInfo.modelData)
          val targetFile = new File(clusterDir, f"${(dist * 100000).toInt}%07d-$fileName%s")
          Files.createLink(targetFile.toPath, faceFile.getAbsoluteFile.toPath.toAbsolutePath)
          val targetFileByDate = new File(clusterDirByDate, s"$fileName")
          Files.createLink(targetFileByDate.toPath, faceFile.getAbsoluteFile.toPath.toAbsolutePath)
        }
    }

  def fileNameForVertexData(vd: VertexData): String = {
    val file = vd.repoFile
    val meta = Settings.manager.metadataFor(Hash.fromString(HashAlgorithm.Sha512, file.getName))
    val fileDateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss")
    val dateStr = MetadataShortcuts.DateTaken(meta).map(dt => fileDateFormat.format(new java.util.Date(dt.clicks))).getOrElse("unknown")
    val fileName = s"$dateStr-${file.getName.take(ShortHash)}-${vd.faceIndex}.jpg"
    fileName
  }
  def ensureFace(vd: VertexData): File = {
    val file = vd.repoFile
    import vd.rectangle
    val fileName = fileNameForVertexData(vd)
    val faceFile = new File(faceDir, fileName)

    if (!faceFile.exists) {
      import sys.process._
      val cmd = s"convert ${file.getAbsolutePath} -crop ${rectangle.width}x${rectangle.height}+${rectangle.left}+${rectangle.top} ${faceFile.getAbsolutePath}"
      cmd.!
      faceFile.setWritable(false)
    }
    faceFile
  }
  def createFaceLinkTo(targetDir: File)(vd: VertexData): Unit = {
    val fileName = fileNameForVertexData(vd)
    val faceFile = ensureFace(vd)
    val targetFile = new File(targetDir, fileName)
    Files.createLink(targetFile.toPath, faceFile.getAbsoluteFile.toPath.toAbsolutePath)
  }

  /*val unrelatedFaces: Set[VertexData] = (candidates.map(vertexData).toSet diff vertices)
  println(s"Saving ${unrelatedFaces.size} unrelated faces to files")
  val unknownDir = new File(s"/tmp/clusters/unknown")
  unknownDir.mkdirs()
  unrelatedFaces.toSeq.foreach(createFaceLinkTo(unknownDir))*/
}