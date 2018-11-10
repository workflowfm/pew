package com.workflowfm.pew.simulation.metrics

import scala.collection.immutable.Queue
import com.workflowfm.pew.metrics.FileOutput

trait SimMetricsOutput extends ((Long,SimMetricsAggregator) => Unit) {
  def and(h:SimMetricsOutput) = SimMetricsOutputs(this,h) 
}
object SimMetricsOutput {
  def formatOption[T](v:Option[T], nullValue: String, format:T=>String={ x:T => x.toString }) = v.map(format).getOrElse(nullValue)
}

case class SimMetricsOutputs(handlers:Queue[SimMetricsOutput]) extends SimMetricsOutput {
  override def apply(time:Long,aggregator:SimMetricsAggregator) = handlers map (_.apply(time,aggregator))
  override def and(h:SimMetricsOutput) = SimMetricsOutputs(handlers :+ h)
}
object SimMetricsOutputs {
  def apply(handlers:SimMetricsOutput*):SimMetricsOutputs = SimMetricsOutputs(Queue[SimMetricsOutput]() ++ handlers)
}


trait SimMetricsStringOutput extends SimMetricsOutput {
  val nullValue = "NULL"
  
  def taskHeader(separator:String) = Seq("ID","Task","Simulation","Created","Start","Delay","Duration","Cost","Resources").mkString(separator)
  def taskCSV(separator:String, resSeparator:String)(m:TaskMetrics) = m match {
    case TaskMetrics(id,task,sim,ct,st,dur,cost,res) => 
      Seq(id,task,sim,ct,SimMetricsOutput.formatOption(st,nullValue),m.delay,dur,cost,res.mkString(resSeparator)).mkString(separator)
  }
  
  def simHeader(separator:String) = Seq("Name","Start","Duration","Delay","Tasks","Cost","Result").mkString(separator)
  def simCSV(separator:String)(m:SimulationMetrics) = m match {
    case SimulationMetrics(name,st,dur,delay,ts,c,res) => 
      Seq(name,st,dur,delay,ts,c,res).mkString(separator)
  }

  def resHeader(separator:String) = Seq("Name","Busy","Idle","Tasks","Cost").mkString(separator)
  def resCSV(separator:String)(m:ResourceMetrics) = m match {
    case ResourceMetrics(name,b,i,ts,c) => 
      Seq(name,b,i,ts,c).mkString(separator)
  }

  
  def tasks(aggregator:SimMetricsAggregator,separator:String,lineSep:String="\n",resSeparator:String=";") = 
    aggregator.taskMetrics.map(taskCSV(separator,resSeparator)).mkString(lineSep)
  def simulations(aggregator:SimMetricsAggregator,separator:String, lineSep:String="\n") = 
    aggregator.simulationMetrics.map(simCSV(separator)).mkString(lineSep)
  def resources(aggregator:SimMetricsAggregator,separator:String, lineSep:String="\n") = 
    aggregator.resourceMetrics.map(resCSV(separator)).mkString(lineSep)
}


class SimMetricsPrinter extends SimMetricsStringOutput {  
  def apply(totalTicks:Long,aggregator:SimMetricsAggregator) = {
    val sep = "\t| "
    val lineSep = "\n"
    println(
s"""
Tasks
-----
${taskHeader(sep)}
${tasks(aggregator,sep,lineSep)}

Simulations
-----------
${simHeader(sep)}
${simulations(aggregator,sep,lineSep)}

Resources
---------
${resHeader(sep)}
${resources(aggregator,sep,lineSep)}
"""
        )
  }
}

class SimCSVFileOutput(path:String,name:String) extends SimMetricsStringOutput with FileOutput {  
  import java.io._

  val separator = ","
  val lineSep = "\n"
  
  def apply(totalTicks:Long,aggregator:SimMetricsAggregator) = {
    val taskFile = s"$path$name-tasks.csv"
    val simulationFile = s"$path$name-simulations.csv"
    val resourceFile = s"$path$name-resources.csv"
    writeToFile(taskFile, taskHeader(separator) + "\n" + tasks(aggregator,separator,lineSep))
    writeToFile(simulationFile, simHeader(separator) + "\n" + simulations(aggregator,separator,lineSep))
    writeToFile(resourceFile, resHeader(separator) + "\n" + resources(aggregator,separator,lineSep))        
  }
}


class SimD3Timeline(path:String,file:String,tick:Int=1) extends SimMetricsOutput with FileOutput {  
  import java.io._
  
  override def apply(totalTicks:Long, aggregator:SimMetricsAggregator) = {
    val result = build(aggregator,System.currentTimeMillis())
    println(result)
    val dataFile = s"$path$file-simdata.js"
    writeToFile(dataFile, result)
  }
  
  def build(aggregator:SimMetricsAggregator, now:Long) = {
    var buf:StringBuilder = StringBuilder.newBuilder
    buf.append("var tasks = [\n")
    for (p <- aggregator.taskSet) buf.append(s"""\t"$p",\n""")
    buf.append("];\n\n")
    buf.append("var resourceData = [\n")
    for (m <- aggregator.resourceMetrics) buf.append(s"""${resourceEntry(m, aggregator)}\n""")
    buf.append("];\n\n")
    buf.append("var simulationData = [\n")
    for (m <- aggregator.simulationMetrics) buf.append(s"""${simulationEntry(m, aggregator)}\n""")
    buf.append("];\n")
    buf.toString
  }
  
  def simulationEntry(s:SimulationMetrics,agg:SimMetricsAggregator) = {
    val times = agg.taskMetricsOf(s).map(taskEntry).mkString(",\n")
s"""{label: "${s.name}", times: [
$times
]},"""
  }
  
  def resourceEntry(res:ResourceMetrics,agg:SimMetricsAggregator) = {
    val times = agg.taskMetricsOf(res).map(taskEntry).mkString(",\n")
s"""{label: "${res.name}", times: [
$times
]},"""
  }

  
  def taskEntry(m:TaskMetrics) = {
    val start = (m.started.getOrElse(1L) - 1L) * tick
    val finish = (m.started.getOrElse(1L) + m.duration - 1L) * tick
    val delay = m.delay * tick
    s"""\t{"label":"${m.fullName}", task: "${m.task}", "id":${m.id}, "starting_time": $start, "ending_time": $finish, delay: $delay, cost: ${m.cost}}"""
  }
}