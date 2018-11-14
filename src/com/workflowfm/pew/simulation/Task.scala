package com.workflowfm.pew.simulation

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Promise
import scala.concurrent.duration._
import com.workflowfm.pew.simulation.metrics._

object Task {
  sealed trait Priority extends Ordered[Priority] {
    def value:Int
    def compare(that:Priority) = this.value - that.value
  }
  case object Highest extends Priority { val value = 5 }
  case object High extends Priority { val value = 4 }
  case object Medium extends Priority { val value = 3 }
  case object Low extends Priority { val value = 2 }
  case object VeryLow extends Priority { val value = 1 }
}

class Task (
    val id:Long, 
    val name:String, 
    val simulation:String, 
    val created:Long,
    val resources:Seq[String], 
    val duration:Long, 
    val estimatedDuration:Long, 
    val initialCost:Long, 
    val interrupt:Int=Int.MaxValue, 
    val priority:Task.Priority=Task.Medium
      ) extends Ordered[Task] {
  
  val promise:Promise[Unit] = Promise()
  
  var cost:Long = initialCost 
  
  // execute will be called once by each associated TaskResource 
  def complete(time:Long) = if (!promise.isCompleted) promise.success(Unit)
  
  def addCost(extra:Long) = cost += extra
  
  def nextPossibleStart(currentTime:Long, resourceMap:Map[String,TaskResource]) = {
    (currentTime /: resources){ case (i,rN) => resourceMap.get(rN) match {
      case None => throw new RuntimeException(s"Resource $rN not found!")
      case Some(r) => Math.max(i,r.nextAvailableTimestamp(currentTime))
    }}
  }

  def taskResources(resourceMap:Map[String,TaskResource]) = resources flatMap (resourceMap.get(_))

  
  def compare(that:Task) = {
    lazy val cPriority = that.priority.compare(this.priority)
    lazy val cResources = that.resources.size.compare(this.resources.size)
    lazy val cAge = this.created.compare(that.created)
    lazy val cDuration = that.estimatedDuration.compare(this.estimatedDuration)
    lazy val cInterrupt = that.interrupt.compare(this.interrupt)
    lazy val cName = this.name.compare(that.name)
    lazy val cSimulation = this.simulation.compare(that.simulation)
    
    if (cPriority != 0) cPriority
    else if (cAge != 0) cAge
    else if (cResources != 0) cResources
    else if (cDuration != 0) cDuration
    else if (cInterrupt != 0) cInterrupt
    else if (cName != 0) cName
    else cSimulation
  }
  
  override def toString = {
    val res = resources.mkString(",")
    s"Task($name)($res)"
  }
}

case class TaskGenerator (
    name :String, 
    simulation:String, 
    duration:ValueGenerator[Long], 
    cost:ValueGenerator[Long], 
    interrupt:Int=(-1), 
    priority:Task.Priority=Task.Medium
      ) {
  def create(id:Long, time:Long, resources:String*) = new Task(id,name,simulation,time,resources,duration.get,duration.estimate,cost.get,interrupt,priority)
  def withPriority(p:Task.Priority) = copy(priority = p)
  def withInterrupt(int:Int) = copy(interrupt = int)
  def withDuration(dur:ValueGenerator[Long]) = copy(duration = dur)
  def withName(n:String) = copy(name = n)
  def withSimulation(s:String) = copy(simulation=s)
  
  def addTo(coordinator:ActorRef, resources:String*)(implicit system: ActorSystem) = {
    //implicit val timeout = Timeout(1.second)
    // change this to ? to require an acknowledgement
    val promise = Promise[Unit]()
    coordinator ! Coordinator.AddTask(this,promise,resources)
    promise.future
  }

}
