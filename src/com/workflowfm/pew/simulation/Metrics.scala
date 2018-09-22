package com.workflowfm.pew.simulation

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import com.workflowfm.pew.execution.FutureExecutor

trait Metrics {
  def stringValues :List[String]
  def values(sep:String=",") = stringValues.mkString(sep)
}

class MetricTracker[T <: Metrics](init :T) {
  var metrics :T = init
  def <~(u:T=>T) :this.type = { synchronized { metrics = u(metrics) } ; this }
}

object TaskMetrics {
  def header(sep:String) = List("Task","Start","Delay","Duration","Cost","Simulation","Resources").mkString(sep)
}
case class TaskMetrics (task:String, start:Int, delay:Int, duration:Int, cost:Int, simulation:String, resources:Seq[String]) extends Metrics {
  override def stringValues = List(task,start,delay,duration,cost,simulation,"\"" + resources.mkString(",") + "\"") map (_.toString)
//  def addDelay(d :Int) = copy(delay = delay + d)
//  def addDuration(d :Int) = copy(duration = duration + d)
//  def addCost(c:Int) = copy(cost = cost + c)
  def setStart(st:Int) = copy(start=st)
  def setResources(sim:String,rs:Seq[String]) = copy(simulation=sim,resources=rs)
  def add(dl:Int, dur:Int, c:Int) = copy(delay = delay + dl, duration = duration + dur, cost = cost + c)
}

class TaskMetricTracker(task:String) extends MetricTracker[TaskMetrics](TaskMetrics(task,-1,0,0,0,"",Seq())) {
  def taskStart(t:Int) = this <~ (_.setStart(t))
  def taskDone(t:Task, time:Int, cost:Int, costPerTick:Int) = if (metrics.start > 0) {
    this <~ (_.add(metrics.start - t.createdTime, time - metrics.start, cost + costPerTick * (time-metrics.start))) <~
    (_.setResources(t.simulation,t.resources))
  }
}

object SimulationMetrics {
  def header(sep:String) = List("Start","Duration","Cost","Result").mkString(sep)
}
case class SimulationMetrics (start:Int, duration: Int, cost: Int, result :String) extends Metrics {
  override def stringValues = List(start,duration,cost,"\"" + result + "\"") map (_.toString)
  def setStart(st:Int) = copy(start=st)
  def setResult(r:String) = copy(result = r) 
  def addDuration(d:Int) = copy(duration = duration + d)
  def addCost(c:Int) = copy(cost = cost + c)
}

class SimulationMetricTracker() extends MetricTracker[SimulationMetrics](SimulationMetrics(-1,0,0,"None")) {
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
  val simulationMetrics = Queue[(String,SimulationMetrics)]()
  val resourceMetrics = Queue[(String,ResourceMetrics)]()
  
  def +=(s:String,m:TaskMetrics) = taskMetrics += (s->m)
  def +=(s:String,m:SimulationMetrics) = simulationMetrics += (s->m)
  def +=(s:String,m:ResourceMetrics) = resourceMetrics += (s->m)
  
  def +=(t:Task) = taskMetrics += ((t.name + "(" + t.simulation + ")")->t.metrics)
  //def +=(s:Simulation) = simulationMetrics += (s.name->s.metrics)
  def +=(r:TaskResource) = resourceMetrics += (r.name->r.metrics)
  
  def values(sep:String)(e:(String,Metrics)) = e._1 + sep + e._2.values(sep)
  
  def taskValues(sep:String) = (taskMetrics map values(sep)).mkString("\n")
  def simulationValues(sep:String) = (simulationMetrics map values(sep)).mkString("\n")
  def resourceValues(sep:String) = (resourceMetrics map values(sep)).mkString("\n")

  def taskTable(sep:String) = 
    "Name" + sep + TaskMetrics.header(sep) + "\n" + taskValues(sep) + "\n"
  def simulationTable(sep:String) =
    "Name" + sep + SimulationMetrics.header(sep) + "\n" + simulationValues(sep) + "\n"
  def resourceTable(sep:String) =
    "Name" + sep + ResourceMetrics.header(sep) + "\n" + resourceValues(sep) + "\n"
}


object MetricsActor {
  case class Start(coordinator:ActorRef)
  case class StartSims(coordinator:ActorRef,sims:Seq[(Int,Simulation)],executor:FutureExecutor)
  case class StartSimsNow(coordinator:ActorRef,sims:Seq[Simulation],executor:FutureExecutor)
  
  def props(m:MetricsOutput): Props = Props(new MetricsActor(m))
}

class MetricsActor(m:MetricsOutput) extends Actor {
  var coordinator:Option[ActorRef] = None
  
  def receive = {
    case MetricsActor.Start(coordinator) if this.coordinator.isEmpty => {
      this.coordinator = Some(coordinator)
      coordinator ! Coordinator.Start
    }
    
    case MetricsActor.StartSims(coordinator,sims,executor) if this.coordinator.isEmpty => {
      this.coordinator = Some(coordinator)
      coordinator ! Coordinator.AddSims(sims,executor)
      coordinator ! Coordinator.Start
    }
    
    case MetricsActor.StartSimsNow(coordinator,sims,executor) if this.coordinator.isEmpty => {
      this.coordinator = Some(coordinator)
      coordinator ! Coordinator.AddSimsNow(sims,executor)
      coordinator ! Coordinator.Start
    }
    
    case Coordinator.Done(t:Int,ma:MetricAggregator) if this.coordinator == Some(sender) => {
      this.coordinator = None
      m(t,ma)
    }
  }
}


trait MetricsOutput extends ((Int,MetricAggregator) => Unit)

case class MetricsOutputs(handlers:MetricsOutput*) extends MetricsOutput {
  def apply(totalTicks:Int,aggregator:MetricAggregator) = handlers map (_.apply(totalTicks,aggregator))
}

class MetricsPrinter extends MetricsOutput {  
  def apply(totalTicks:Int,aggregator:MetricAggregator) = {
    val sep = "\t| "
    println(
        "Tasks\n" +
        "-----\n" +
        aggregator.taskTable(sep) + "\n" +
        "Simulations\n" +
        "-----------\n" +
        aggregator.simulationTable(sep) + "\n" +
        "Resources\n" +
        "---------\n" +
        aggregator.resourceTable(sep) + "\n\n"
        )
  }
}

class MetricsCSVFileOutput(path:String,name:String) extends MetricsOutput {  
  import java.io._
  
  def apply(totalTicks:Int,aggregator:MetricAggregator) = {
    val sep = ","
    val taskFile = s"$path$name-tasks.csv"
    val simulationFile = s"$path$name-simulations.csv"
    val resourceFile = s"$path$name-resources.csv"
    writeToFile(taskFile, aggregator.taskTable(sep) + "\n")
    writeToFile(simulationFile, aggregator.simulationTable(sep) + "\n")
    writeToFile(resourceFile, aggregator.resourceTable(sep) + "\n")        
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

class MetricsPythonGantt(path:String,name:String) extends MetricsOutput {  
  import java.io._
  import sys.process._
  
  def apply(totalTicks:Int,aggregator:MetricAggregator) = {
    val sep = ","
    val taskFile = s"$path$name.csv"
    writeToFile(taskFile, aggregator.taskTable(sep) + "\n") match {
      case None => Unit
      case Some(f) => {
        val osname = System.getProperty("os.name").toLowerCase()
        val script = new File(getClass.getResource("/python/pappilib-gantt.py").toURI()).getAbsolutePath
        val cmd = (if (osname.contains("win")) s"python.exe $script " else s"$script ") + f.getAbsolutePath
        println(s"*** Executing: $cmd")
        println(cmd !!)
      }
    }
      
  }
  
  def writeToFile(filePath:String,output:String) :Option[File] = try {
    val file = new File(filePath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(output)
    bw.close()
    Some(file)
  } catch {
    case e:Exception => e.printStackTrace()
    None
  }
}

class MetricsD3Timeline(path:String,name:String,tick:Int=1) extends MetricsOutput {  
  import java.io._
  import sys.process._
  
  def apply(totalTicks:Int,aggregator:MetricAggregator) = {
    val result = build(totalTicks,aggregator)
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
  
  def build(totalTicks:Int,aggregator:MetricAggregator) = {
    var buf:StringBuilder = StringBuilder.newBuilder
    buf.append(s"var totalTicks = $totalTicks\n")
    buf.append("\nvar tasks = [\n")
    for (t <- aggregator.taskMetrics.map(_._2.task).toSet[String]) buf.append(s"""\t"$t",\n""")
    buf.append("];\n\nvar resourceData = [\n")
    for (r <- aggregator.resourceMetrics.sortWith(sortRes)) buf.append(resourceEntry(r._1,aggregator))
    buf.append("];\n\nvar simulationData = [\n")
    for (s <- aggregator.simulationMetrics.sortWith(sortSim)) buf.append(simulationEntry(s._1,aggregator))
    buf.append("];\n")
    buf.toString
  }
  
  def sortSim(l:(String,SimulationMetrics),r:(String,SimulationMetrics)) = 
    (l._2.start.compareTo(r._2.start)) match {
    case 0 => l._1.compareTo(r._1) < 0
    case c => c < 0
  }

  
  def simulationEntry(sim:String,agg:MetricAggregator) = {
    val tasks = agg.taskMetrics.filter(_._2.simulation==sim)
    val times = ("" /: tasks)(_ + "\t" + taskEntry(_))
    s"""{label: \"$sim\", times: [""" + "\n" + times + "]},\n"
  }
  
  def sortRes(l:(String,ResourceMetrics),r:(String,ResourceMetrics)) =
		(l._2.start.compareTo(r._2.start)) match {
    case 0 => l._1.compareTo(r._1) < 0
    case c => c < 0
  }
  
  def resourceEntry(res:String,agg:MetricAggregator) = {
    val tasks = agg.taskMetrics.filter(_._2.resources.contains(res))
    val times = ("" /: tasks)(_ + "\t" + taskEntry(_))
    s"""{label: \"$res\", times: [""" + "\n" + times + "]},\n"
  }
  
  def taskEntry(entry:(String,TaskMetrics)) = entry match { case (name,metrics) =>
    val start = metrics.start * tick
    val finish = (metrics.start + metrics.duration) * tick
    val task = metrics.task
    s"""{"label":"$name", task: "$task", "starting_time": $start, "ending_time": $finish},\n"""
    
  }
}