package com.workflowfm.pew.metrics

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import com.workflowfm.pew.execution.FutureExecutor
import com.workflowfm.pew.simulation.Simulation
import com.workflowfm.pew.simulation.Coordinator


object MetricsActor {
  case class Start(coordinator:ActorRef)
  case class StartSims(coordinator:ActorRef,sims:Seq[(Int,Simulation)],executor:FutureExecutor)
  case class StartSimsNow(coordinator:ActorRef,sims:Seq[Simulation],executor:FutureExecutor)
  
  def props(m:MetricsOutput, callbackActor:Option[ActorRef]=None)(implicit system: ActorSystem): Props = Props(new MetricsActor(m,callbackActor)(system))
}

// Provide a callbackActor to get a response when we are done. Otherwise we'll shutdown the ActorSystem 

class MetricsActor(m:MetricsOutput, callbackActor:Option[ActorRef])(implicit system: ActorSystem) extends Actor {
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
      callbackActor match {
        case None => system.terminate()
        case Some(actor) => actor ! Coordinator.Done(t,ma)
      }
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
    val start = (metrics.start - 1) * tick
    val finish = (metrics.start + metrics.duration - 1) * tick
    val task = metrics.task
    val delay = metrics.delay * tick
    val cost = metrics.cost
    s"""{"label":"$name", task: "$task", "starting_time": $start, "ending_time": $finish, delay: $delay, cost: $cost},\n"""
    
  }
}