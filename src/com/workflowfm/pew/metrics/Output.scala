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
  
  def apply(aggregator:MetricsAggregator[KeyT]) = {
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

//class MetricsD3Timeline(path:String,name:String,tick:Int=1) extends MetricsOutput {  
//  import java.io._
//  import sys.process._
//  
//  def apply(totalTicks:Int,aggregator:MetricAggregator) = {
//    val result = build(totalTicks,aggregator)
//    println(result)
//    val dataFile = s"$path$name-data.js"
//    writeToFile(dataFile, result)
//  }
//  
//  def writeToFile(filePath:String,output:String) = try {
//    val file = new File(filePath)
//    val bw = new BufferedWriter(new FileWriter(file))
//    bw.write(output)
//    bw.close()
//  } catch {
//    case e:Exception => e.printStackTrace()
//  }
//  
//  def build(totalTicks:Int,aggregator:MetricAggregator) = {
//    var buf:StringBuilder = StringBuilder.newBuilder
//    buf.append(s"var totalTicks = $totalTicks\n")
//    buf.append("\nvar tasks = [\n")
//    for (t <- aggregator.taskMetrics.map(_._2.task).toSet[String]) buf.append(s"""\t"$t",\n""")
//    buf.append("];\n\nvar resourceData = [\n")
//    for (r <- aggregator.resourceMetrics.sortWith(sortRes)) buf.append(resourceEntry(r._1,aggregator))
//    buf.append("];\n\nvar workflowData = [\n")
//    for (s <- aggregator.workflowMetrics.sortWith(sortWf)) buf.append(workflowEntry(s._1,aggregator))
//    buf.append("];\n")
//    buf.toString
//  }
//  
//  def sortWf(l:(String,WorkflowMetrics),r:(String,WorkflowMetrics)) = 
//    (l._2.start.compareTo(r._2.start)) match {
//    case 0 => l._1.compareTo(r._1) < 0
//    case c => c < 0
//  }
//
//  
//  def workflowEntry(wf:String,agg:MetricAggregator) = {
//    val tasks = agg.taskMetrics.filter(_._2.workflow==wf)
//    val times = ("" /: tasks)(_ + "\t" + taskEntry(_))
//    s"""{label: \"$wf\", times: [""" + "\n" + times + "]},\n"
//  }
//  
//  def sortRes(l:(String,ResourceMetrics),r:(String,ResourceMetrics)) =
//		(l._2.start.compareTo(r._2.start)) match {
//    case 0 => l._1.compareTo(r._1) < 0
//    case c => c < 0
//  }
//  
//  def resourceEntry(res:String,agg:MetricAggregator) = {
//    val tasks = agg.taskMetrics.filter(_._2.resources.contains(res))
//    val times = ("" /: tasks)(_ + "\t" + taskEntry(_))
//    s"""{label: \"$res\", times: [""" + "\n" + times + "]},\n"
//  }
//  
//  def taskEntry(entry:(String,TaskMetrics)) = entry match { case (name,metrics) =>
//    val start = (metrics.start - 1) * tick
//    val finish = (metrics.start + metrics.duration - 1) * tick
//    val task = metrics.task
//    val delay = metrics.delay * tick
//    val cost = metrics.cost
//    s"""{"label":"$name", task: "$task", "starting_time": $start, "ending_time": $finish, delay: $delay, cost: $cost},\n"""
//    
//  }
//}