package com.workflowfm.pew.metrics

import java.text.SimpleDateFormat

import org.apache.commons.lang3.time.DurationFormatUtils

/** Contains helpful formatting shortcut functions. */
trait MetricsFormatting {

  def formatOption[T](
      v: Option[T],
      nullValue: String,
      format: T => String = { x: T => x.toString }
  ): String = v.map(format).getOrElse(nullValue)
  def formatTime(format: String)(time: Long): String = new SimpleDateFormat(format).format(time)

  def formatTimeOption(time: Option[Long], format: String, nullValue: String): String =
    formatOption(time, nullValue, formatTime(format))

  def formatDuration(from: Long, to: Long, format: String): String =
    DurationFormatUtils.formatDuration(to - from, format)

  def formatDuration(from: Option[Long], to: Long, format: String, nullValue: String): String =
    from.map { f =>
      DurationFormatUtils.formatDuration(to - f, format).toString
    } getOrElse (nullValue)

  def formatDuration(
      from: Option[Long],
      to: Option[Long],
      format: String,
      nullValue: String
  ): String =
    from.map { f =>
      to.map { t =>
        DurationFormatUtils.formatDuration(t - f, format).toString
      } getOrElse (nullValue)
    } getOrElse (nullValue)


  // don't use interpolation, it's problematic
  // see https://github.com/scala/bug/issues/6476
  def quote(s: String): String = "\"" + s + "\"" 
}
