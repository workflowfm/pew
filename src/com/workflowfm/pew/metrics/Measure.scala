package com.workflowfm.pew.metrics


import com.workflowfm.pew.execution.ProcessExecutor
import com.workflowfm.pew.simulation.Coordinator
import com.workflowfm.pew.simulation.Simulation
import com.workflowfm.pew.simulation.Task
import com.workflowfm.pew.simulation.TaskResource
import scala.collection.Seq

trait Metrics {
  def stringValues :List[String]
  def values(sep:String=",") = stringValues.mkString(sep)
}

class MetricTracker[T <: Metrics](init :T) {
  var metrics :T = init
  def <~(u:T=>T) :this.type = { synchronized { metrics = u(metrics) } ; this }
}

object TaskMetrics {
  def header(sep:String) = List("Task","Start","Delay","Duration","Cost","Workflow","Resources").mkString(sep)
}
case class TaskMetrics (task:String, start:Int, delay:Int, duration:Int, cost:Int, workflow:String, resources:Seq[String]) extends Metrics {
  override def stringValues = List(task,start,delay,duration,cost,workflow,"\"" + resources.mkString(",") + "\"") map (_.toString)
//  def addDelay(d :Int) = copy(delay = delay + d)
//  def addDuration(d :Int) = copy(duration = duration + d)
//  def addCost(c:Int) = copy(cost = cost + c)
  def setStart(st:Int) = copy(start=st)
  def setResources(wf:String,rs:Seq[String]) = copy(workflow=wf,resources=rs)
  def add(dl:Int, dur:Int, c:Int) = copy(delay = delay + dl, duration = duration + dur, cost = cost + c)
}

class TaskMetricTracker(task:String) extends MetricTracker[TaskMetrics](TaskMetrics(task,-1,0,0,0,"",Seq())) {
  def taskStart(t:Int) = this <~ (_.setStart(t))
  def taskDone(t:Task, time:Int, cost:Int, costPerTick:Int) = if (metrics.start > 0) {
    this <~ (_.add(metrics.start - t.createdTime, time - metrics.start, cost + costPerTick * (time-metrics.start))) <~
    (_.setResources(t.simulation,t.resources))
  }
}

object WorkflowMetrics {
  def header(sep:String) = List("Start","Duration","Cost","Result").mkString(sep)
}
case class WorkflowMetrics (start:Int, duration: Int, cost: Int, result :String) extends Metrics {
  override def stringValues = List(start,duration,cost,"\"" + result + "\"") map (_.toString)
  def setStart(st:Int) = copy(start=st)
  def setResult(r:String) = copy(result = r) 
  def addDuration(d:Int) = copy(duration = duration + d)
  def addCost(c:Int) = copy(cost = cost + c)
}

class WorkflowMetricTracker() extends MetricTracker[WorkflowMetrics](WorkflowMetrics(-1,0,0,"None")) {
  def simStart(t:Int) = this <~ (_.setStart(t))
  def simDone(t:Int) = if (metrics.start > 0) {
    this <~ (_.addDuration(t - metrics.start)) 
  } else this
  def taskDone(tm:TaskMetrics) = this <~ (_.addCost(tm.cost)) 
  def setResult(r:String) = this <~ (_.setResult(r)) 
}


object ResourceMetrics {
  def header(sep:String) = List("Start","Busy","Idle","Tasks","Cost").mkString(sep)
}
case class ResourceMetrics (start:Int, busy:Int, idle:Int, tasks:Int, cost:Int) extends Metrics {
  override def stringValues = List(start,busy,idle,tasks,cost) map (_.toString)
  def setStart(t:Int) = if (start < 0) copy(start=t) else this
  def addBusy(b:Int) = copy(busy = busy + b)
  def addIdle(i:Int) = copy(idle = idle + i)
  def addTask = copy(tasks = tasks + 1)
  def addCost(c:Int) = copy(cost = cost + c)
}

class ResourceMetricTracker() extends MetricTracker[ResourceMetrics](ResourceMetrics(-1,0,0,0,0)) { 
  def resStart(t:Int) = this <~ (_.setStart(t))
  def idle(t:Int) = this <~ (_.addIdle(t)) 
  def taskDone(tm:TaskMetrics,costPerTick:Int) = this <~ (_.addCost(tm.duration * costPerTick)) <~ (_.addBusy(tm.duration)) <~ (_.addTask)  
}


class MetricAggregator() {
  import scala.collection.mutable.Queue
  val taskMetrics = Queue[(String,TaskMetrics)]()
  val workflowMetrics = Queue[(String,WorkflowMetrics)]()
  val resourceMetrics = Queue[(String,ResourceMetrics)]()
  
  def +=(s:String,m:TaskMetrics) = taskMetrics += (s->m)
  def +=(s:String,m:WorkflowMetrics) = workflowMetrics += (s->m)
  def +=(s:String,m:ResourceMetrics) = resourceMetrics += (s->m)
  
  def +=(t:Task) = taskMetrics += ((t.name + "(" + t.simulation + ")")->t.metrics)
  //def +=(s:Simulation) = simulationMetrics += (s.name->s.metrics)
  def +=(r:TaskResource) = resourceMetrics += (r.name->r.metrics)
  
  def values(sep:String)(e:(String,Metrics)) = e._1 + sep + e._2.values(sep)
  
  def taskValues(sep:String) = (taskMetrics map values(sep)).mkString("\n")
  def workflowValues(sep:String) = (workflowMetrics map values(sep)).mkString("\n")
  def resourceValues(sep:String) = (resourceMetrics map values(sep)).mkString("\n")

  def taskTable(sep:String) = 
    "Name" + sep + TaskMetrics.header(sep) + "\n" + taskValues(sep) + "\n"
  def workflowTable(sep:String) =
    "Name" + sep + WorkflowMetrics.header(sep) + "\n" + workflowValues(sep) + "\n"
  def resourceTable(sep:String) =
    "Name" + sep + ResourceMetrics.header(sep) + "\n" + resourceValues(sep) + "\n"
}


