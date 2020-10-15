package com.workflowfm.pew.metrics

import scala.collection.immutable.Queue

/** Manipulates a [[MetricsAggregator]] to produce some output via side-effects.
  * @tparam KeyT the type used for workflow IDs
  */
trait MetricsOutput[KeyT] extends (MetricsAggregator[KeyT] => Unit) {
  /** Compose with another [[MetricsOutput]] in sequence. */
  def and(h: MetricsOutput[KeyT]): MetricsOutputs[KeyT] = MetricsOutputs(this, h)
}

/** A [[MetricsOutput]] consisting of a [[scala.collection.immutable.Queue]] of [[MetricsOutput]]s
  * to be run sequentially.
  */
case class MetricsOutputs[KeyT](handlers: Queue[MetricsOutput[KeyT]]) extends MetricsOutput[KeyT] {
  /** Call all included [[MetricsOutput]]s. */
  override def apply(aggregator: MetricsAggregator[KeyT]): Unit = handlers map (_.apply(aggregator))
  /** Add another [[MetricsOutput]] in sequence. */
  override def and(h: MetricsOutput[KeyT]): MetricsOutputs[KeyT] = MetricsOutputs(handlers :+ h)
}

object MetricsOutputs {

  /** Shorthand constructor for a [[MetricsOutputs]] from a list of [[MetricsOutput]]s. */
  def apply[KeyT](handlers: MetricsOutput[KeyT]*): MetricsOutputs[KeyT] =
    MetricsOutputs[KeyT](Queue[MetricsOutput[KeyT]]() ++ handlers)
}

/** Generates a string representation of the metrics using a generalized CSV format. */
trait MetricsStringOutput[KeyT] extends MetricsOutput[KeyT] with MetricsFormatting {
  /** A string representing null values. */
  val nullValue = "NULL"

  /** The field names for [[ProcessMetrics]].
    * @param separator a string (such as a space or comma) to separate the names
    */
  def procHeader(separator: String): String =
    Seq("ID", "PID", "Process", "Start", "Finish", "Result").mkString(separator)

  /** String representation of a [[ProcessMetrics]] instance.
    *
    * @param separator a string (such as a space or comma) to separate the values
    * @param timeFormat optional argument to format timestamps using `java.text.SimpleDateFormat`
    * @param m the [[ProcessMetrics]] instance to be handled
    */
  def procCSV(separator: String, timeFormat: Option[String])(m: ProcessMetrics[KeyT]): String =
    m match {
      case ProcessMetrics(id, r, p, s, f, res) =>
        timeFormat match {
          case None =>
            Seq(
              id,
              r,
              p,
              s,
              formatOption(f, nullValue),
              formatOption(res, nullValue)
            ).mkString(separator)
          case Some(format) =>
            Seq(
              id,
              r,
              p,
              formatTime(format)(s),
              formatTimeOption(f, format, nullValue),
              formatOption(res, nullValue)
            ).mkString(separator)
        }
    }

  /** The field names for [[WorkflowMetrics]].
    * @param separator a string (such as a space or comma) to separate the names
    */
  def workflowHeader(separator: String): String =
    Seq("ID", "PID", "Process", "Start", "Finish", "Result").mkString(separator)

  /** String representation of a [[WorkflowMetrics]] instance.
    *
    * @param separator a string (such as a space or comma) to separate the values
    * @param timeFormat optional argument to format timestamps using `java.text.SimpleDateFormat`
    * @param m the [[WorkflowMetrics]] instance to be handled
    */
  def workflowCSV(separator: String, timeFormat: Option[String])(m: WorkflowMetrics[KeyT]): String =
    m match {
      case WorkflowMetrics(id, s, c, f, res) =>
        timeFormat match {
          case None =>
            Seq(
              id,
              s,
              c,
              formatOption(f, nullValue),
              formatOption(res, nullValue)
            ).mkString(separator)
          case Some(format) =>
            Seq(
              id,
              formatTime(format)(s),
              c,
              formatTimeOption(f, format, nullValue),
              formatOption(res, nullValue)
            ).mkString(separator)
        }
    }

  /** Formats all [[ProcessMetrics]] in a [[MetricsAggregator]] in a single string.
    *
    * @param aggregator the [[MetricsAggregator]] to retrieve the metrics to be formatted
    * @param separator a string (such as a space or comma) to separate values
    * @param lineSep a string (such as a new line) to separate process metrics
    * @param timeFormat optional argument to format timestamps using `java.text.SimpleDateFormat`
    */
  def processes(
      aggregator: MetricsAggregator[KeyT],
      separator: String,
      lineSep: String = "\n",
      timeFormat: Option[String] = None
  ): String =
    aggregator.processMetrics.map(procCSV(separator, timeFormat)).mkString(lineSep)

  /** Formats all [[WorkflowMetrics]] in a [[MetricsAggregator]] in a single string.
    *
    * @param aggregator the [[MetricsAggregator]] to retrieve the metrics to be formatted
    * @param separator a string (such as a space or comma) to separate values
    * @param lineSep a string (such as a new line) to separate workflow metrics
    * @param timeFormat optional argument to format timestamps using `java.text.SimpleDateFormat`
    */
  def workflows(
      aggregator: MetricsAggregator[KeyT],
      separator: String,
      lineSep: String = "\n",
      timeFormat: Option[String] = None
  ): String =
    aggregator.workflowMetrics.map(workflowCSV(separator, timeFormat)).mkString(lineSep)
}

/** Prints all metrics to standard output. */
class MetricsPrinter[KeyT] extends MetricsStringOutput[KeyT] {
  /** Separates the values. */
  val separator = "\t| "
  /** Separates metrics instances. */
  val lineSep = "\n"
  /** Default time format using `java.text.SimpleDateFormat`. */
  val timeFormat: Some[String] = Some("YYYY-MM-dd HH:mm:ss.SSS")

  override def apply(aggregator: MetricsAggregator[KeyT]): Unit = {
    println(
      s"""
Tasks
-----
${procHeader(separator)}
${processes(aggregator, separator, lineSep, timeFormat)}

Workflows
---------
${workflowHeader(separator)}
${workflows(aggregator, separator, lineSep, timeFormat)}
"""
    )
  }
}

/** Helper to write stuff to a file.
  * @todo Move to [[com.workflowfm.pew.util]]
  */
trait FileOutput {
  import java.io._

  def writeToFile(filePath: String, output: String): Unit = try {
    val file = new File(filePath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(output)
    bw.close()
  } catch {
    case e: Exception => e.printStackTrace()
  }
}

/** Outputs metrics to files using a standard CSV format.
  * Generates 2 CSV files:
  * 1. one for processes with a "-tasks.csv" suffix,
  * 2. and one for workflows with a "-workflows.csv" suffix.
  *
  * @tparam KeyT the type used for workflow IDs
  * @param path path to directory where the files will be placed
  * @param name file name prefix
  */
class MetricsCSVFileOutput[KeyT](path: String, name: String)
    extends MetricsStringOutput[KeyT]
    with FileOutput {

  val separator = ","

  override def apply(aggregator: MetricsAggregator[KeyT]): Unit = {
    val taskFile = s"$path$name-tasks.csv"
    val workflowFile = s"$path$name-workflows.csv"
    writeToFile(taskFile, procHeader(separator) + "\n" + processes(aggregator, separator) + "\n")
    writeToFile(
      workflowFile,
      workflowHeader(separator) + "\n" + workflows(aggregator, separator) + "\n"
    )
  }
}

/** Outputs metrics to a file using the d3-timeline format.
  * Generates 1 file with a "-data.js" suffix.
  * This can then be combined with the resources at
  * [[https://github.com/PetrosPapapa/WorkflowFM-PEW/tree/master/resources/d3-timeline]]
  * to render the timeline in a browser.
  *
  * @tparam KeyT the type used for workflow IDs
  * @param path path to directory where the files will be placed
  * @param file file name prefix
  */
class MetricsD3Timeline[KeyT](path: String, file: String)
    extends MetricsOutput[KeyT]
    with FileOutput
    with MetricsFormatting {

  override def apply(aggregator: MetricsAggregator[KeyT]): Unit = {
    val result = build(aggregator, System.currentTimeMillis())
    println(result)
    val dataFile = s"$path$file-data.js"
    writeToFile(dataFile, result)
  }

  /** Helps build the output with a static system time. */
  def build(aggregator: MetricsAggregator[KeyT], now: Long): String = {
    val buf: StringBuilder = StringBuilder.newBuilder
    buf.append("var processes = [\n")
    for (p <- aggregator.processSet) buf.append(s"""\t"$p",\n""")
    buf.append("];\n\n")
    buf.append("var workflowData = [\n")
    for (m <- aggregator.workflowMetrics)
      buf.append(s"""${workflowEntry(m, aggregator, now, "\t")}\n""")
    buf.append("];\n")
    buf.toString
  }

  /** Encodes an entire workflow as a timeline.
    *
    * @return the encoded timeline for the workflow
    * @param m thr [[WorkflowMetrics]] recorded for the particular workflow
    * @param agg the [[MetricsAggregator]] containing all the relevant metrics
    * @param now the current (real) to be used as the end time of unfinished processes
    * @param prefix a string to prefix (usually some whitespace) to prefix the entry
    */
  def workflowEntry(
      m: WorkflowMetrics[KeyT],
      agg: MetricsAggregator[KeyT],
      now: Long,
      prefix: String
  ): String = {
    val processes = (Map[String, Queue[ProcessMetrics[KeyT]]]() /: agg.processMetricsOf(m.piID)) {
      case (m, p) => {
        val procs = m.getOrElse(p.process, Queue()) :+ p
        m + (p.process -> procs)
      }
    }
    val data = processes.map { case (proc, i) => processEntry(proc, i, now, prefix + "\t") }
    s"""$prefix{id: \"${m.piID}\", data: [\n${data.mkString("")}$prefix]},\n"""
  }

  /** Encodes multiple process calls of the same process in a single lane.
    *
    * @return the encoded timeline lane
    * @param proc the name of the process
    * @param i the list of [[ProcessMetrics]] recorded for each process call
    * @param now the current (real) to be used as the end time of unfinished processes
    * @param prefix a string to prefix (usually some whitespace) to prefix the entry
    */
  def processEntry(
      proc: String,
      i: Seq[ProcessMetrics[KeyT]],
      now: Long,
      prefix: String
  ): String = {
    if (i.isEmpty) ""
    else {
      val times = ("" /: i) { case (s, m) => s"$s${callEntry(now, m, prefix + "\t")}" }
      s"""$prefix{label: \"$proc\", times: [\n$times$prefix]},\n"""
    }
  }

  /** Encodes a process call as a timeline task.
    *
    * @return the encoded timeline task
    * @param now the current (real) to be used as the end time of unfinished processes
    * @param m the [[ProcessMetrics]] recorded for this process call
    * @param prefix a string to prefix (usually some whitespace) to prefix the entry
    */
  def callEntry(now: Long, m: ProcessMetrics[KeyT], prefix: String): String = {
    s"""$prefix{"label":"${m.ref}", "process": "${m.process}", "starting_time": ${m.start}, "ending_time": ${m.finish
      .getOrElse(now)}, "result":"${formatOption(m.result, "NONE")}"},\n"""

  }
}
