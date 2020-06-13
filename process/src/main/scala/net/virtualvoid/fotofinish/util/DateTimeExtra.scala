package net.virtualvoid.fotofinish.util

import akka.http.scaladsl.model.DateTime

object DateTimeExtra {
  implicit class RichDateTime(dateTime: DateTime) {
    def fromNow: String = {
      val millis = DateTime.now.clicks - dateTime.clicks
      val days0 = millis / 1000 / 3600 / 24
      val years = days0 / 365
      val days1 = days0 % 365
      val months = days1 / 30
      val days = days1 % 30

      def x(num: Long, what: String): String =
        if (num > 0) s"$num $what " else ""

      x(years, "years") + x(months, "months") + x(days, "days") + "ago"
    }
  }
}
