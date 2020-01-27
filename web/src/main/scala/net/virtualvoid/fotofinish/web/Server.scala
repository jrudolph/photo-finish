package net.virtualvoid.fotofinish
package web

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.{ ExceptionHandler, PathMatcher, PathMatcher1, Route }
import play.twirl.api.Html
import util.ImageTools
import html._
import metadata._

import scala.concurrent.Future
import scala.util.control.NonFatal

object Server extends App {
  implicit val system = ActorSystem()
  import system.dispatcher

  val app = MetadataApp(Settings.config)
  val binding = Http().bindAndHandle(
    ServerRoutes.route(app),
    "localhost", 8654
  )
  binding.onComplete { res =>
    println(s"Binding now $res")
  }
}

object ServerRoutes {
  def route(app: MetadataApp): Route =
    new ServerRoutes(app).main
}

private[web] class ServerRoutes(app: MetadataApp) {
  import app.executionContext
  import TwirlSupport._
  import akka.http.scaladsl.server.Directives._

  lazy val main: Route =
    handleExceptions(exceptionHandler) {
      concat(
        pathPrefix("images")(images),
        pathPrefix("gallery")(gallery),
        pathPrefix("extractors")(extractors),
        auxiliary,
      )
    }

  lazy val images: Route =
    concat(
      pathPrefix(app.config.hashAlgorithm.name) {
        concat(
          pathPrefix(FileInfoByDefaultHash) { fileInfo =>
            onSuccess(app.metadataFor(fileInfo.id)) { meta =>
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
            }
          },
          pathPrefix(HashPrefix) { prefix =>
            extractUri { uri =>
              onSuccess(app.completeIdPrefix(Id.generic("sha-512-t160", prefix))) {
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

  lazy val gallery: Route =
    get {
      val imageDatasF =
        for {
          objs <- app.knownObjects()
          imageDatas <- Future.traverse(objs.take(100).toVector)(imageDataForId).map(_.toSeq.flatten)
        } yield imageDatas

      onSuccess(imageDatasF) { imageDatas =>
        complete(Gallery(imageDatas))
      }
    }

  lazy val extractors: Route =
    get {
      onSuccess(app.extractorStatus()) { status =>
        complete(ExtractorStatus(status))
      }
    }

  lazy val auxiliary: Route =
    getFromResourceDirectory("web")

  val HashPrefix = PathMatcher(s"[0-9a-fA-F]{2,${HashAlgorithm.Sha512.hexStringLength - 1}}".r)

  val FileInfoByDefaultHash: PathMatcher1[FileInfo] =
    PathMatcher(s"[0-9a-fA-F]{${app.config.hashAlgorithm.hexStringLength}}".r).map { hash =>
      app.config.fileInfoOf(Hash.fromString(app.config.hashAlgorithm, hash))
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

  def imageDataForId(id: Id): Future[Option[GalleriaImageData]] = app.metadataFor(id).map { meta =>
    val imageBase = s"/images/${id.hash.hashAlgorithm.name}/${id.hash.asHexString}/"

    import MetadataShortcuts._

    val description = s"${id.hash.asHexString.take(10)} ${DateTaken(meta).getOrElse("")} ${CameraModel(meta).getOrElse("")}"

    Thumbnail(meta).map { _ =>
      GalleriaImageData(imageBase + "oriented", imageBase + "thumbnail", imageBase, description)
    }
  }

  def exceptionHandler: ExceptionHandler = ExceptionHandler {
    case NonFatal(ex) =>
      ex.printStackTrace()
      throw ex
  }
}

case class GalleriaImageData(image: String, thumb: String, link: String, description: String)
object GalleriaImageData {
  import spray.json._
  import DefaultJsonProtocol._
  implicit val imageDataFormat = jsonFormat4(GalleriaImageData.apply _)
}