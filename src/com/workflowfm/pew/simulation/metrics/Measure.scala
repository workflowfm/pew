package com.workflowfm.pew.simulation.metrics

import com.workflowfm.pew.simulation._


//trait Metrics {
//  def stringValues :List[String]
//  def values(sep:String=",") = stringValues.mkString(sep)
//}
//
//class MetricTracker[T <: Metrics](init :T) {
//  var metrics :T = init
//  def <~(u:T=>T) :this.type = { synchronized { metrics = u(metrics) } ; this }
//}
//
//object TaskMetrics {
//  def header(sep:String) = List("Task","Start","Delay","Duration","Cost","Workflow","Resources").mkString(sep)
//}
case class TaskMetrics (id:Long, task:String, simulation:String, created:Int, started:Option[Int], duration:Int, cost:Int, resources:Seq[String]) {
  //override def stringValues = List(task,start,delay,duration,cost,workflow,"\"" + resources.mkString(",") + "\"") map (_.toString)
//  def addDelay(d :Int) = copy(delay = delay + d)
//  def addDuration(d :Int) = copy(duration = duration + d)
//  def addCost(c:Int) = copy(cost = cost + c)
  def start(st:Int) = copy(started=Some(st))
  def done(t:Task, time:Int, cost:Int, costPerTick:Int) = {
    val st = started match {
      case None => time
      case Some(t) => t
    }
    copy( duration = duration + time - st, cost = cost + costPerTick * (time-st), resources = t.resources)
  }
}
object TaskMetrics {
  def apply(task:Task):TaskMetrics = TaskMetrics(task.id, task.name, task.simulation, task.createdTime, None, 0, 0, Seq())
}

case class SimulationMetrics(name:String, started:Int, duration:Int, cost:Int, result:Option[String]) {
//  override def stringValues = List(start,duration,cost,"\"" + result + "\"") map (_.toString)
  def result(r:String) = copy(result = Some(r)) 
  def addDuration(d:Int) = copy(duration = duration + d)
  def addCost(c:Int) = copy(cost = cost + c)
  def taskDone(tm:TaskMetrics) = copy(cost = cost + tm.cost) 
  def done(time:Int) = copy( duration = duration + time - started ) // TODO should this and result be one?
}
object SimulationMetrics {
  def apply(name:String, t:Int):SimulationMetrics = SimulationMetrics(name,t,0,0,None) 
}

case class ResourceMetrics (name:String, started:Option[Int], busyTime:Int, idleTime:Int, tasks:Int, cost:Int) {
  def start(t:Int) = copy(started=Some(t)) // TODO do we need this or just init?
  def idle(i:Int) = copy(idleTime = idleTime + i)
  def taskDone(tm:TaskMetrics,costPerTick:Int) = copy(
      tasks = tasks + 1, 
      cost = cost + tm.duration * costPerTick, 
      busyTime = busyTime + tm.duration
  )  
}
object ResourceMetrics {
  def apply(name:String):ResourceMetrics = ResourceMetrics(name,None,0,0,0,0) 
  def apply(r:TaskResource):ResourceMetrics = ResourceMetrics(r.name,None,0,0,0,0) 
}

class SimMetricsAggregator {
  import scala.collection.immutable.Map
  
  val taskMap = scala.collection.mutable.Map[Long,TaskMetrics]()
  val simMap = scala.collection.mutable.Map[String,SimulationMetrics]()
  val resourceMap = scala.collection.mutable.Map[String,ResourceMetrics]()

  // Set 
  
  def +=(m:TaskMetrics):Unit = taskMap += (m.id->m)
  def +=(m:SimulationMetrics):Unit = simMap += (m.name->m)
  def +=(m:ResourceMetrics):Unit = resourceMap += (m.name->m)
  
  def +=(t:Task):Unit = this += TaskMetrics(t)
  //def +=(s:Simulation):Unit = simulationMetrics += (s.name->s.metrics)
  def +=(r:TaskResource):Unit = resourceMap += (r.name->ResourceMetrics(r))

  
  // Update
  
  def ^(taskID:Long)(u:TaskMetrics=>TaskMetrics) = 
    taskMap.get(taskID).map { m => this += u(m) }
  def ^^(simulation:String)(u:SimulationMetrics=>SimulationMetrics) = 
    simMap.get(simulation).map { m => this += u(m) }
  def ^(resource:String)(u:ResourceMetrics=>ResourceMetrics) = 
    resourceMap.get(resource).map { m => this += u(m) }

  // TODO events + getters
  
}



//
//  def taskValues(sep:String) = (taskMetrics map values(sep)).mkString("\n")
//  def workflowValues(sep:String) = (workflowMetrics map values(sep)).mkString("\n")
//  def resourceValues(sep:String) = (resourceMetrics map values(sep)).mkString("\n")
//
//  def taskTable(sep:String) = 
//    "Name" + sep + TaskMetrics.header(sep) + "\n" + taskValues(sep) + "\n"
//  def workflowTable(sep:String) =
//    "Name" + sep + WorkflowMetrics.header(sep) + "\n" + workflowValues(sep) + "\n"
//  def resourceTable(sep:String) =
//    "Name" + sep + ResourceMetrics.header(sep) + "\n" + resourceValues(sep) + "\n"
