package com.workflowfm.pew.metrics

import scala.collection.immutable.Queue
import java.text.SimpleDateFormat

trait MetricsOutput[KeyT] extends (MetricsAggregator[KeyT] => Unit) {
  def and(h:MetricsOutput[KeyT]) = MetricsOutputs(this,h) 
}
object MetricsOutput {
  def formatOption[T](v:Option[T], nullValue: String, format:T=>String={ x:T => x.toString }) = v.map(format).getOrElse(nullValue)
  def formatTime(format:String)(time:Long) = new SimpleDateFormat(format).format(time/1000L)
  def formatTimeOption(time:Option[Long], format:String, nullValue:String) = formatOption(time, nullValue, formatTime(format))  
}

case class MetricsOutputs[KeyT](handlers:Queue[MetricsOutput[KeyT]]) extends MetricsOutput[KeyT] {
  override def apply(aggregator:MetricsAggregator[KeyT]) = handlers map (_.apply(aggregator))
  override def and(h:MetricsOutput[KeyT]) = MetricsOutputs(handlers :+ h)
}
object MetricsOutputs {
  def apply[KeyT](handlers:MetricsOutput[KeyT]*):MetricsOutputs[KeyT] = MetricsOutputs[KeyT](Queue[MetricsOutput[KeyT]]() ++ handlers)
}


trait MetricsStringOutput[KeyT] extends MetricsOutput[KeyT] {
  val nullValue = "NULL"
  
  def procHeader(separator:String) = Seq("ID","PID","Process","Start","Finish","Result").mkString(separator)
  def procCSV(separator:String,timeFormat:Option[String])(m:ProcessMetrics[KeyT]) = m match {
    case ProcessMetrics(id,r,p,s,f,res) => timeFormat match {
      case None => Seq(id,r,p,s,MetricsOutput.formatOption(f,nullValue),MetricsOutput.formatOption(res,nullValue)).mkString(separator)
      case Some(format) => Seq(id,r,p,MetricsOutput.formatTime(format)(s),MetricsOutput.formatTimeOption(f,nullValue,format),MetricsOutput.formatOption(res,nullValue)).mkString(separator)
    }
  }
  
  def workflowHeader(separator:String) = Seq("ID","PID","Process","Start","Finish","Result").mkString(separator)
  def workflowCSV(separator:String,timeFormat:Option[String])(m:WorkflowMetrics[KeyT]) = m match {
    case WorkflowMetrics(id,s,c,f,res) => timeFormat match {
      case None => Seq(id,s,c,MetricsOutput.formatOption(f,nullValue),MetricsOutput.formatOption(res,nullValue)).mkString(separator)
      case Some(format) => Seq(id,MetricsOutput.formatTime(format)(s),c,MetricsOutput.formatTimeOption(f,nullValue,format),MetricsOutput.formatOption(res,nullValue)).mkString(separator)
    }
  }
  
  def processes(aggregator:MetricsAggregator[KeyT],separator:String,timeFormat:Option[String]=None) = 
    aggregator.processMetrics.map(procCSV(separator,timeFormat)).mkString("\n")
  def workflows(aggregator:MetricsAggregator[KeyT],separator:String,timeFormat:Option[String]=None) = 
    aggregator.workflowMetrics.map(workflowCSV(separator,timeFormat)).mkString("\n")
}



class MetricsPrinter[KeyT] extends MetricsStringOutput[KeyT] {  
  val separator = "\t| "
  val timeFormat = Some("YYYY-MM-dd HH:mm:ss.SSS")
  
  override def apply(aggregator:MetricsAggregator[KeyT]) = { 
    println(
        "Tasks\n" +
        "-----\n" +
        processes(aggregator,separator,timeFormat) + "\n" +
        "Workflows\n" +
        "-----------\n" +
        workflows(aggregator,separator,timeFormat) + "\n\n"
        )
  }
}

class MetricsCSVFileOutput[KeyT](path:String,name:String) extends MetricsStringOutput[KeyT] {  
  import java.io._
  
  val separator = ","
  
  override def apply(aggregator:MetricsAggregator[KeyT]) = {
    val taskFile = s"$path$name-tasks.csv"
    val workflowFile = s"$path$name-workflows.csv"
    writeToFile(taskFile, processes(aggregator,separator) + "\n")
    writeToFile(workflowFile, workflows(aggregator,separator) + "\n")      
  }
  
  def writeToFile(filePath:String,output:String) = try {
    val file = new File(filePath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(output)
    bw.close()
  } catch {
    case e:Exception => e.printStackTrace()
  }
}

class MetricsD3Timeline[KeyT](path:String,name:String,tick:Int=1) extends MetricsOutput[KeyT] {  
  import java.io._
  import sys.process._
  
  override def apply(aggregator:MetricsAggregator[KeyT]) = {
    val result = build(aggregator,System.nanoTime())
    println(result)
    val dataFile = s"$path$name-data.js"
    writeToFile(dataFile, result)
  }
  
  def writeToFile(filePath:String,output:String) = try {
    val file = new File(filePath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(output)
    bw.close()
  } catch {
    case e:Exception => e.printStackTrace()
  }
  
  def build(aggregator:MetricsAggregator[KeyT], now:Long) = {
    var buf:StringBuilder = StringBuilder.newBuilder
    buf.append("var processes = [\n")
    for (p <- aggregator.processSet) buf.append(s"""\t"$p",\n""")
    buf.append("];\n\n")
    buf.append("var data = [\n")
    for (m <- aggregator.workflowMetrics) buf.append(s"""\t${workflowEntry(m, aggregator, now, "\t")},\n""")
    buf.append("];\n")
    buf.toString
  }
  
  def workflowEntry(m:WorkflowMetrics[KeyT], agg:MetricsAggregator[KeyT], now:Long, prefix:String) = {
    val processes = (Map[String,Queue[ProcessMetrics[KeyT]]]() /: agg.processMetricsOf(m.piID)){ case (m,p) => {
        val procs = m.getOrElse(p.process, Queue()) :+ p
        m + (p.process->procs) 
      } }
    val data = processes.foreach { case (proc,i) => processEntry(proc, i, now, prefix + "\t") }
    s"""$prefix{id: \"${m.piID}\", data: $data},\n"""
  }
  
  def processEntry(proc:String, i:Seq[ProcessMetrics[KeyT]], now:Long, prefix:String) = { 
    if (i.isEmpty) "" else {   
      val times = ("" /: i){ case (s,m) => s"$s${callEntry(now,m,prefix + "\t")}" }
      s"""$prefix{label: \"$proc\", times: [\n$times]},\n"""
    }
  }
  
  def callEntry(now:Long, m:ProcessMetrics[KeyT], prefix:String) = {
    s"""$prefix{"label":"${m.ref}", "process": "${m.process}", "starting_time": ${m.start/1000L}, "ending_time": ${m.finish.getOrElse(now)/1000L}, "result":"${MetricsOutput.formatOption(m.result,"NONE")}"},\n"""
    
  }
}