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
case class TaskMetrics (
    id:Long, 
    task:String, 
    simulation:String, 
    created:Long, 
    started:Option[Long], 
    duration:Long, 
    cost:Long, 
    resources:Seq[String]
      ) {
  //override def stringValues = List(task,start,delay,duration,cost,workflow,"\"" + resources.mkString(",") + "\"") map (_.toString)
//  def addDelay(d :Long) = copy(delay = delay + d)
//  def addDuration(d :Long) = copy(duration = duration + d)
//  def addCost(c:Long) = copy(cost = cost + c)
  def start(st:Long) = copy(started=Some(st))
//  def done(t:Task, time:Long, cost:Long, costPerTick:Long) = {
//    val st = started match {
//      case None => time
//      case Some(t) => t
//    }
//    copy( duration = duration + time - st, cost = cost + costPerTick * (time-st), resources = t.resources)
//  }
}
object TaskMetrics {
  def apply(task:Task):TaskMetrics = TaskMetrics(task.id, task.name, task.simulation, task.createdTime, None, task.duration, task.cost, task.resources)
}

case class SimulationMetrics(name:String, started:Long, duration:Long, tasks:Int, cost:Long, result:Option[String]) {
//  override def stringValues = List(start,duration,cost,"\"" + result + "\"") map (_.toString)
  def addDuration(d:Long) = copy(duration = duration + d)
  def addCost(c:Long) = copy(cost = cost + c)
  def task(task:Task) = copy(tasks = tasks + 1, cost = cost + task.cost) 
  def done(res:String, time:Long) = copy( result = Some(res), duration = duration + time - started ) // TODO should this and result be one?
}
object SimulationMetrics {
  def apply(name:String, t:Long):SimulationMetrics = SimulationMetrics(name,t,0L,0,0L,None) 
}

case class ResourceMetrics (name:String, busyTime:Long, idleTime:Long, tasks:Int, cost:Long) {
  def idle(i:Long) = copy(idleTime = idleTime + i)
  def task(task:Task, costPerTick:Long) = copy(
      tasks = tasks + 1, 
      cost = cost + task.duration * costPerTick, 
      busyTime = busyTime + task.duration
  )  
}
object ResourceMetrics {
  def apply(name:String):ResourceMetrics = ResourceMetrics(name,0L,0L,0,0L) 
  def apply(r:TaskResource):ResourceMetrics = ResourceMetrics(r.name,0L,0L,0,0L) 
}

class SimMetricsAggregator {
  import scala.collection.immutable.Map
  
  val taskMap = scala.collection.mutable.Map[Long,TaskMetrics]()
  val simMap = scala.collection.mutable.Map[String,SimulationMetrics]()
  val resourceMap = scala.collection.mutable.Map[String,ResourceMetrics]()

  // Set 
  
  def +=(m:TaskMetrics):TaskMetrics = { taskMap += (m.id->m) ; m }
  def +=(m:SimulationMetrics):SimulationMetrics = { simMap += (m.name->m) ; m }
  def +=(m:ResourceMetrics):ResourceMetrics = { resourceMap += (m.name->m) ; m }
  
  def +=(task:Task):TaskMetrics = this += TaskMetrics(task)
  def +=(s:Simulation,t:Long):SimulationMetrics = this += SimulationMetrics(s.name,t)
  def +=(r:TaskResource):ResourceMetrics = this += ResourceMetrics(r)

  
  // Update
  
  def ^(taskID:Long)(u:TaskMetrics=>TaskMetrics):Option[TaskMetrics] = 
    taskMap.get(taskID).map { m => this += u(m) }
  def ^(task:Task)(u:TaskMetrics=>TaskMetrics):Option[TaskMetrics] = 
    taskMap.get(task.id).map { m => this += u(m) }
  def ^(simulation:Simulation)(u:SimulationMetrics=>SimulationMetrics):Option[SimulationMetrics] = 
    simMap.get(simulation.name).map { m => this += u(m) }
  def ^(simulationName:String)(u:SimulationMetrics=>SimulationMetrics):Option[SimulationMetrics] = 
    simMap.get(simulationName).map { m => this += u(m) }
//  def ^(resource:String)(u:ResourceMetrics=>ResourceMetrics):Option[ResourceMetrics] = 
//    resourceMap.get(resource).map { m => this += u(m) }
  def ^(resource:TaskResource)(u:ResourceMetrics=>ResourceMetrics):Option[ResourceMetrics] = 
    resourceMap.get(resource.name).map { m => this += u(m) }
  
  // TODO  getters
  
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
