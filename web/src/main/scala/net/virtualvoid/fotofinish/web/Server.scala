package net.virtualvoid.fotofinish
package web

import java.io.File

import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{ ContentType, DateTime, HttpEntity, MediaTypes, StatusCodes, Uri }
import org.apache.pekko.http.scaladsl.server.{ Directive0, ExceptionHandler, PathMatcher, PathMatcher1, Route }
import org.apache.pekko.stream.IOResult
import org.apache.pekko.stream.scaladsl.{ FileIO, Flow, Keep }
import org.apache.pekko.util.ByteString
import play.twirl.api.Html
import util.ImageTools
import util.DateTimeExtra._
import html._
import metadata._
import net.virtualvoid.fotofinish.metadata.Id.Hashed
import net.virtualvoid.fotofinish.process.{ HierarchyAccess, Node }

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.util.Success
import scala.util.control.NonFatal

object Server extends App {
  implicit val system = ActorSystem()
  import system.dispatcher

  val app = MetadataApp(Settings.config)
  val binding =
    Http().newServerAt("localhost", 8654)
      .bind(ServerRoutes.route(app))
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
  import org.apache.pekko.http.scaladsl.server.Directives._

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

  lazy val faceCache = hashSuffixCache("faces", MediaTypes.`image/jpeg`.toContentType)
  lazy val thumbnailCache = hashSuffixCache("thumbnails", MediaTypes.`image/jpeg`.toContentType)
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
                      ImageTools.correctOrientation(MetadataShortcuts.Orientation(meta).getOrElse(Orientation.Normal))(fileInfo.repoFile)
                    )
                  }
                },
                path("thumbnail") {
                  val thumbnailType = "square150"
                  thumbnailCache(fileInfo.hash, thumbnailType) {
                    val thumbData = ImageTools.squareThumbnailIM(150, MetadataShortcuts.Orientation(meta).getOrElse(Orientation.Normal))(fileInfo.repoFile)

                    complete {
                      HttpEntity(MediaTypes.`image/jpeg`, thumbData)
                    }
                  }
                },
                pathPrefix("face" / IntNumber) { i =>
                  concat(
                    pathEndOrSingleSlash {
                      faceCache(fileInfo.hash, i.toString) {
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
                        def dateTaken(hash: Hash): Future[Option[DateTime]] =
                          app.metadataFor(Hashed(hash)).map { meta =>
                            MetadataShortcuts.DateTaken(meta)
                          }

                        def title(entry: (Hash, Int, Float), dateTaken: Option[DateTime]): String = {
                          val dateString = dateTaken.fold("")(dt => s"${dt.fromNow} $dt ")
                          s"${dateString}distance: ${entry._3}"
                        }

                        def annotateNeighbor(entry: (Hash, Int, Float)): Future[FaceNeighbor] =
                          dateTaken(entry._1).map { dateTaken =>
                            val t = title(entry, dateTaken)
                            FaceNeighbor(entry._1, entry._2, entry._3, t, dateTaken)
                          }

                        onSuccess(Future.traverse(neighbors)(annotateNeighbor)) { annotatedNeighbors =>
                          complete {
                            MetadataShortcuts.Faces(meta).lift(i).map { thisFace =>
                              FaceInfoPage(thisFace, annotatedNeighbors)
                            }
                          }
                        }
                      }
                    }
                  )
                },
                path("metadata") {
                  complete(MetadataInfo(fileInfo, meta))
                },
                redirectToTrailingSlashIfMissing(StatusCodes.Found) {
                  pathSingleSlash {
                    onSuccess(app.phashApi.similarImagesTo(fileInfo.hash)) { similar =>
                      complete(ImageInfo(fileInfo, meta, fields(fileInfo, meta), similar))
                    }
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
    onSuccess(app.knownObjects()) { objs =>
      galleryRouteForIds(objs, 100)
    }

  def galleryRouteForIds(ids: Iterable[Id], maxNumber: Int = 10000): Route =
    get {
      onSuccess(Future.traverse(ids.take(maxNumber).toVector)(imageDataForId)) { imageDatas =>
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
              parameter("gallery".?) { gallery =>
                if (gallery.isDefined)
                  galleryRouteForIds(entries.toVector.sortBy(_._1).flatMap(_._2.map(Hashed.apply)))
                else
                  complete(Hierarchy(node.name, node.fullPath.mkString("/"), entries.toVector.sortBy(_._1), children.map(c => c._1 -> c._2.numEntries).toVector.sorted))
              }
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
      shortcut(metadata).map { t => name -> display(t) }.toSeq

    val ingestion = metadata.getValues[IngestionData]

    def formatIngestionData(d: IngestionData): String = {
      @tailrec def createLink(remainingSegments: Seq[String], linkPrefix: String, result: String): String = remainingSegments match {
        case Nil         => result
        case head +: Nil => result + "/" + head
        case head +: tail =>
          val link = linkPrefix + "/" + head
          createLink(tail, link, result + s"""/<a href="$link">$head</a>""")
      }

      s"Original Path: ${createLink(d.originalFullFilePath.split("/").toVector.drop(1), "/by-filename", "")}"
    }

    import MetadataShortcuts._
    Seq(
      "Hash" -> Html(fileInfo.hash.asHexString),
      "File Size" -> Html(ingestion.head.fileSize.toString)
    ) ++
      fromOptional("Mime Type", MimeType)(d => Html(d)) ++
      fromOptional("Width", Width)(d => Html(d.toString)) ++
      fromOptional("Height", Height)(d => Html(d.toString)) ++
      fromOptional("Orientation", Orientation)(o => Html(o.toString)) ++
      fromOptional("Date Taken", DateTaken)(d => Html(dateLink(d))) ++
      fromOptional("Camera Model", CameraModel)(m => Html(m)) ++
      Seq(
        "Thumbnail" -> Html("""<img src="thumbnail" />"""),
        "Ingestion Data" -> Html(ingestion.map(formatIngestionData).mkString("<br/>"))
      )
  }

  def dateLink(d: DateTime): String = {
    import d._
    f"""<a href="/by-date/$year%4d">$year%4d</a>-<a href="/by-date/$year%4d/$month%02d">$month%02d</a>-$day%02d $hour%02d:$minute%02d:$second%02d"""
  }

  def imageDataForId(id: Id): Future[GalleriaImageData] = app.metadataFor(id).map { meta =>
    val imageBase = s"/images/${id.hash.hashAlgorithm.name}/${id.hash.asHexString}/"

    import MetadataShortcuts._

    val description = s"${id.hash.asHexString.take(10)} ${DateTaken(meta).getOrElse("")} ${CameraModel(meta).getOrElse("")}"

    GalleriaImageData(imageBase + "oriented", imageBase + "thumbnail", imageBase, description)
  }

  def exceptionHandler: ExceptionHandler = ExceptionHandler {
    case NonFatal(ex) =>
      ex.printStackTrace()
      throw ex
  }

  def hashSuffixCache(subdir: String, contentType: ContentType): (Hash, String) => Directive0 = {
    val c = cache(subdir, contentType)

    (hash, suffix) => c(s"${hash.asHexString.take(2)}/${hash.asHexString.drop(2)}.$suffix.${contentType.mediaType.fileExtensions.head}")
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