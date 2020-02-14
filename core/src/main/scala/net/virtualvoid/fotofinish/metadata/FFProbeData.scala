package net.virtualvoid.fotofinish.metadata

import spray.json._

case class FFProbeData(
    ffprobeOutput: JsValue
)
object FFProbeData extends MetadataKind.Impl[FFProbeData]("net.virtualvoid.fotofinish.metadata.FFProbeData", 1) {
  import spray.json.DefaultJsonProtocol._
  override implicit def jsonFormat: JsonFormat[FFProbeData] = jsonFormat1(FFProbeData.apply _)
}

object FFProbeDataExtractor {
  def instance: MetadataExtractor =
    ImageDataExtractor.fromFileSync("net.virtualvoid.fotofinish.metadata.FFProbeDataExtractor", 1, FFProbeData, mimeTypeFilter = _.startsWith("video/")) { file =>
      import sys.process._
      val data = s"""ffprobe -v quiet -print_format json -show_versions -show_format -show_streams ${file.getCanonicalPath}""".!!
      FFProbeData(data.parseJson)
    }
}