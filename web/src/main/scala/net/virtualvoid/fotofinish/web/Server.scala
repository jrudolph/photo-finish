package net.virtualvoid.fotofinish
package web

import java.io.File

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ ContentType, HttpEntity, MediaTypes, StatusCodes, Uri }
import akka.http.scaladsl.server.{ Directive0, ExceptionHandler, PathMatcher, PathMatcher1, Route }
import akka.stream.IOResult
import akka.stream.scaladsl.{ FileIO, Flow, Keep }
import akka.util.ByteString
import play.twirl.api.Html
import util.ImageTools
import util.DateTimeExtra._
import html._
import metadata._
import net.virtualvoid.fotofinish.metadata.Id.Hashed
import net.virtualvoid.fotofinish.process.{ HierarchyAccess, Node }

import scala.concurrent.Future
import scala.util.Success
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
        pathPrefix("views")(views),
        pathPrefix("by-date")(hierarchy(app.byYearMonth)),
        pathPrefix("by-filename")(hierarchy(app.byOriginalFileName)),
        auxiliary,
      )
    }

  lazy val faceCache = cache("faces", MediaTypes.`image/jpeg`.toContentType)
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
                pathPrefix("face" / IntNumber) { i =>
                  concat(
                    pathEndOrSingleSlash {
                      faceCache(s"${fileInfo.hash.asHexString.take(2)}/${fileInfo.hash.asHexString.drop(2)}.$i.jpeg") {
                        complete {
                          val faceData = meta.get[FaceData]
                          faceData.flatMap { data =>
                            data.faces.lift(i).map { face =>
                              import ImageTools.WithRecovery
                              HttpEntity(
                                MediaTypes.`image/jpeg`,
                                ImageTools.cropJpegTran(face.rectangle)
                                  .and(ImageTools.correctOrientationJpegTran(data.orientation.getOrElse(Orientation.Normal)))(fileInfo.repoFile))
                            }
                          }
                        }
                      }
                    },
                    path("info"./) {
                      onSuccess(app.faceApi.similarFacesTo(fileInfo.hash, i)) { neighbors =>
                        def hashInfo(hash: Hash): Future[String] =
                          app.metadataFor(Hashed(hash)).map { meta =>
                            MetadataShortcuts.DateTaken(meta).fold("")(dt => s"${dt.fromNow} $dt ")
                          }
                        def title(entry: (Hash, Int, Float)): Future[String] =
                          hashInfo(entry._1).map(hi => s"${hi}distance: ${entry._3}")
                        def mapEntry(entry: (Hash, Int, Float)): Future[(Hash, Int, Float, String)] = title(entry).map(t => (entry._1, entry._2, entry._3, t))

                        onSuccess(Future.traverse(neighbors)(mapEntry)) { annotatedNeighbors =>
                          complete {
                            meta.get(MetadataShortcuts.Faces).lift(i).map { thisFace =>
                              FaceInfoPage(thisFace, annotatedNeighbors)
                            }
                          }
                        }
                      }
                    }
                  )
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

  lazy val views =
    concat(
      pathPrefix("extractors")(extractors),
      pathPrefix("recent-faces")(latestFaces),
    )

  lazy val extractors: Route =
    get {
      onSuccess(app.extractorStatus()) { status =>
        complete(ExtractorStatus(status))
      }
    }

  lazy val latestFaces: Route =
    get {
      onSuccess(app.mostRecentlyFoundFaces()) { faces =>
        complete(RecentFaces(faces))
      }
    }

  def hierarchy(access: HierarchyAccess[String]): Route =
    (get & redirectToTrailingSlashIfMissing(StatusCodes.Found)) {
      def forNode(nodeF: Future[Option[Node[String]]]): Route =
        onSuccess(nodeF) {
          case Some(node) =>
            val es = node.entries
            val ch = node.children
            (onSuccess(ch) & onSuccess(es)) { (children, entries) =>
              complete(Hierarchy(node.name, node.fullPath.mkString("/"), entries.toVector.sortBy(_._1), children.map(c => c._1 -> c._2.numEntries).toVector.sorted))
            }
          case None => reject
        }

      concat(
        pathEndOrSingleSlash(forNode(access.root.map(Some(_)))),
        path(Segments./) { segments =>
          forNode(access.byPrefix(segments.toVector))
        }
      )
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

  def cache(subdir: String, contentType: ContentType): String => Directive0 = {
    val cacheDir = new File(app.config.cacheDir, subdir)

    key => {
      val cacheFile = new File(cacheDir, key)
      cacheFile.getParentFile.mkdirs()
      //println(s"Cache file: $cacheFile, exists: ${cacheFile.exists}")
      if (cacheFile.exists)
        complete(HttpEntity(contentType, FileIO.fromPath(cacheFile.toPath)))
      else {
        mapResponseEntity { entity =>
          val tmpFile = new File(cacheFile.getAbsolutePath + ".tmp")
          entity
            .transformDataBytes(
              Flow[ByteString]
                .alsoToMat(FileIO.toPath(tmpFile.toPath))(Keep.right)
                .mapMaterializedValue { res =>
                  res.onComplete {
                    case Success(IOResult(count, Success(Done))) =>
                      //println(s"Cache file [$cacheFile] written successfully ${count} bytes")
                      tmpFile.renameTo(cacheFile)
                    case x =>
                      println(s"Cache file [$cacheFile] could not be written successfully because $x")
                      tmpFile.delete()
                  }
                }
            )
        }
      }
    }
  }
}

case class GalleriaImageData(image: String, thumb: String, link: String, description: String)
object GalleriaImageData {
  import spray.json._
  import DefaultJsonProtocol._
  implicit val imageDataFormat = jsonFormat4(GalleriaImageData.apply _)
}