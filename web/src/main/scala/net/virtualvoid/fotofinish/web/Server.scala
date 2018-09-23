package net.virtualvoid.fotofinish
package web

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.PathMatcher
import akka.http.scaladsl.server.PathMatcher1
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import net.virtualvoid.fotofinish.metadata._
import net.virtualvoid.fotofinish.util.ImageTools
import net.virtualvoid.fotofinish.web.html.ImageInfo
import play.twirl.api.Html

object Server extends App {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  import system.dispatcher

  val binding = Http().bindAndHandle(
    ServerRoutes.route(Settings.manager),
    "localhost", 8654
  )
  binding.onComplete { res =>
    println(s"Binding now $res")
  }
}

object ServerRoutes {
  def route(manager: RepositoryManager): Route =
    new ServerRoutes(manager).main
}

private[web] class ServerRoutes(manager: RepositoryManager) {
  import TwirlSupport._
  import akka.http.scaladsl.server.Directives._

  lazy val main: Route =
    pathPrefix("images")(images)

  val HashPrefix = PathMatcher(s"[0-9a-fA-F]{2,${HashAlgorithm.Sha512.hexStringLength - 1}}".r)

  val FileInfoBySha512Hash: PathMatcher1[FileInfo] =
    PathMatcher(s"[0-9a-fA-F]{${HashAlgorithm.Sha512.hexStringLength}}".r).map { hash =>
      manager.config.fileInfoOf(Hash.fromString(HashAlgorithm.Sha512, hash))
    }

  def fields(fileInfo: FileInfo, metadata: Metadata): Seq[(String, Html)] = {
    //def from[T](name: String, shortcut: MetadataShortcuts.ShortCut[T])(display: T => Html):
    def fromOptional[T](name: String, shortcut: MetadataShortcuts.ShortCut[Option[T]])(display: T => Html): Seq[(String, Html)] =
      metadata.get(shortcut).map { t => name -> display(t) }.toSeq

    val ingestion = metadata.getValues[IngestionData]

    def formatIngestionData(d: IngestionData): String =
      s"Original Path: ${d.originalFullFilePath}"

    import MetadataShortcuts._
    Seq(
      "Hash" -> Html(fileInfo.hash.asHexString)
    ) ++
      fromOptional("Width", Width)(d => Html(d.toString)) ++
      fromOptional("Height", Height)(d => Html(d.toString)) ++
      fromOptional("Orientation", Orientation)(o => Html(o.toString)) ++
      fromOptional("Date Taken", DateTaken)(d => Html(d.toString)) ++
      fromOptional("Camera Model", CameraModel)(m => Html(m)) ++
      Seq(
        "Thumbnail" -> Html("""<img src="thumbnail" />"""),
        "Ingestion Data" -> Html(ingestion.map(formatIngestionData).mkString("<br/>"))
      )
  }

  lazy val images: Route =
    concat(
      pathPrefix("sha512") {
        concat(
          pathPrefix(FileInfoBySha512Hash) { fileInfo =>
            import fileInfo.hash
            val meta = manager.metadataFor(hash)

            concat(
              path("raw") {
                getFromFile(fileInfo.repoFile, MediaTypes.`image/jpeg`)
              },
              path("oriented") {
                complete {
                  HttpEntity(
                    MediaTypes.`image/jpeg`,
                    ImageTools.correctOrientation(meta.get(MetadataShortcuts.Orientation).getOrElse(Orientation.Normal))(fileInfo.repoFile)
                  )
                }
              },
              path("thumbnail") {
                complete {
                  meta.get(MetadataShortcuts.Thumbnail).map { thumbData =>
                    HttpEntity(MediaTypes.`image/jpeg`, thumbData)
                  }
                }
              },
              path("face" / IntNumber) { i =>
                complete {
                  meta.get(MetadataShortcuts.Faces).lift(i).map { face =>
                    HttpEntity(
                      MediaTypes.`image/jpeg`,
                      ImageTools.crop(face.rectangle)(fileInfo.repoFile))
                  }
                }
              },
              redirectToTrailingSlashIfMissing(StatusCodes.Found) {
                pathSingleSlash {
                  complete(ImageInfo(fileInfo, meta, fields(fileInfo, meta)))
                }
              }
            )
          },
          pathPrefix(HashPrefix) { prefix =>
            extractUri { uri =>
              manager.config.fileInfoByHashPrefix(prefix) match {
                case Some(fileInfo) =>
                  val newUri = uri.withPath(Uri.Path(uri.path.toString.replace(prefix, fileInfo.hash.asHexString)))
                  redirect(newUri, StatusCodes.Found)
                case None =>
                  reject
              }
            }
          }
        )
      }
    )

}
