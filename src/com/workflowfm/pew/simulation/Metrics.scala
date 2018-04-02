package com.workflowfm.pew.simulation

trait Metrics {
  def stringValues :List[String]
  def values(sep:String=",") = stringValues.mkString(sep)
}

class MetricTracker[T <: Metrics](init :T) {
  var metrics :T = init
  def <~(u:T=>T) :this.type = { synchronized { metrics = u(metrics) } ; this }
}

object TaskMetrics {
  def header(sep:String) = List("Start","Delay","Duration","Cost","Simulation","Resources").mkString(sep)
}
case class TaskMetrics (start:Int, delay:Int, duration:Int, cost:Int, simulation:String, resources:Seq[String], counted:Boolean=false) extends Metrics {
  val stringValues = List(start,delay,duration,cost,simulation,"\"" + resources.mkString(",") + "\"") map (_.toString)
//  def addDelay(d :Int) = copy(delay = delay + d)
//  def addDuration(d :Int) = copy(duration = duration + d)
//  def addCost(c:Int) = copy(cost = cost + c)
  def setStart(st:Int) = copy(start=st)
  def setResources(sim:String,rs:Seq[String]) = copy(simulation=sim,resources=rs)
  def add(dl:Int, dur:Int, c:Int) = copy(delay = delay + dl, duration = duration + dur, cost = cost + c, counted = true)
}

class TaskMetricTracker() extends MetricTracker[TaskMetrics](TaskMetrics(-1,0,0,0,"",Seq())) {
  def taskStart(t:Int) = this <~ (_.setStart(t))
  def taskDone(t:Task, time:Int, cost:Int, costPerTick:Int) = if (metrics.start > 0) {
    this <~ (_.add(metrics.start - t.createdTime, time - metrics.start, cost + costPerTick * (time-metrics.start))) <~
    (_.setResources(t.simulation,t.resources))
  }
}

object SimulationMetrics {
  def header(sep:String) = List("Duration","Cost","Result").mkString(sep)
}
case class SimulationMetrics (duration: Int, cost: Int, result :String) extends Metrics {
  val stringValues = List(duration,cost,"\"" + result + "\"") map (_.toString)
  def setResult(r:String) = copy(result = r) 
  def addDuration(d:Int) = copy(duration = duration + d)
  def addCost(c:Int) = copy(cost = cost + c)
}

class SimulationMetricTracker() extends MetricTracker[SimulationMetrics](SimulationMetrics(0,0,"None")) {
  var timeStarted = -1
  
  def simStart(t:Int) = timeStarted = t
  def simDone(t:Int) = if (timeStarted > 0) {
    this <~ (_.addDuration(t - timeStarted)) 
  }
  def taskDone(tm:TaskMetrics) = this <~ (_.addCost(tm.cost)) 
  def setResult(r:String) = this <~ (_.setResult(r)) 
}


object ResourceMetrics {
  def header(sep:String) = List("Busy","Idle","Tasks","Cost").mkString(sep)
}
case class ResourceMetrics (busy:Int, idle:Int, tasks:Int, cost:Int) extends Metrics {
  val stringValues = List(busy,idle,tasks,cost) map (_.toString)
  def addBusy(b:Int) = copy(busy = busy + b)
  def addIdle(i:Int) = copy(idle = idle + i)
  def addTask = copy(tasks = tasks + 1)
  def addCost(c:Int) = copy(cost = cost + c)
}

class ResourceMetricTracker() extends MetricTracker[ResourceMetrics](ResourceMetrics(0,0,0,0)) { 
  def idleTick() = this <~ (_.addIdle(1)) 
  def taskDone(tm:TaskMetrics,costPerTick:Int) = this <~ (_.addCost(tm.duration * costPerTick)) <~ (_.addBusy(tm.duration)) <~ (_.addTask)  
}


class MetricAggregator() {
  import scala.collection.mutable.Map
  val taskMetrics = Map[String,TaskMetrics]()
  val simulationMetrics = Map[String,SimulationMetrics]()
  val resourceMetrics = Map[String,ResourceMetrics]()
  
  def +=(s:String,m:TaskMetrics) = taskMetrics += (s->m)
  def +=(s:String,m:SimulationMetrics) = simulationMetrics += (s->m)
  def +=(s:String,m:ResourceMetrics) = resourceMetrics += (s->m)
  
  def +=(t:Task) = taskMetrics += ((t.name + "(" + t.simulation + ")")->t.metrics)
  def +=(s:Simulation) = simulationMetrics += (s.name->s.metrics)
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