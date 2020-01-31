package net.virtualvoid.fotofinish.metadata
import java.io.FileInputStream
import java.security.MessageDigest

import spray.json.JsonFormat

import scala.annotation.tailrec
import scala.concurrent.Future

final case class HashData(
    hashes: Map[String, String]
)
object HashData extends MetadataKind.Impl[HashData]("net.virtualvoid.fotofinish.metadata.HashData", 1) {
  import spray.json.DefaultJsonProtocol._
  override implicit val jsonFormat: JsonFormat[HashData] = jsonFormat1(HashData.apply _)
}

object HashDataExtractor {
  val HashTypes = Vector(
    "sha-512",
    "sha-256",
    "sha1",
    "md5"
  )

  val instance =
    MetadataExtractor("net.virtualvoid.fotofinish.metadata.HashDataExtractor", 1, HashData) { (hash, ctx) =>
      ctx.accessData(hash) { file =>
        Future {
          val mds = HashTypes.map(MessageDigest.getInstance)

          val fos = new FileInputStream(file)
          try {
            val buffer = new Array[Byte](128 * 1024)

            @tailrec def rec(): HashData = {
              val read = fos.read(buffer)
              if (read > 0) {
                mds.foreach(_.update(buffer, 0, read))
                rec()
              } else {
                val res =
                  HashData {
                    HashTypes.lazyZip(mds.map(_.digest().map(_ formatted "%02x").mkString)).toMap
                  }
                res
              }
            }

            rec()
          } finally fos.close()
        }(ctx.executionContext)
      }
    }
}