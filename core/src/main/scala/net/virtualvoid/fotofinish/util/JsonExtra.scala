package net.virtualvoid.fotofinish.util

import spray.json.JsObject
import spray.json.JsValue

object JsonExtra {
  implicit class RichJsValue(val jsValue: JsValue) extends AnyVal {
    def +(field: (String, JsValue)): JsObject = {
      val obj = jsValue.asJsObject
      obj.copy(fields = obj.fields + field)
    }
    def field(name: String): JsValue =
      jsValue.asJsObject.fields(name)
  }
}

