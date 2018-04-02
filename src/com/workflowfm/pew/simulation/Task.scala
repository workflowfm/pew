package com.workflowfm.pew.simulation

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Promise
import scala.concurrent.duration._

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

class Task (val name:String, val simulation:String, val resources:Seq[String], result:Any, val duration:ValueGenerator[Int], costGenerator:ValueGenerator[Int], val interrupt:Int=Int.MaxValue, val priority:Task.Priority=Task.Medium) extends TaskMetricTracker(name) with Ordered[Task] {
  
  val cost = costGenerator.get
  val promise:Promise[Any] = Promise()
  
  protected var creationTime :Int = -1
  var executed: Boolean = false

  // execute will be called once by each associated TaskResource 
  def execute(time:Int) = if (!promise.isCompleted) promise.success(result)
  
  def created(time :Int) = if (creationTime < 0) creationTime = time
  def createdTime :Int = creationTime
  
  def addTo(coordinator:ActorRef)(implicit system: ActorSystem) = {
    //implicit val timeout = Timeout(1.second)
    // use ? to require an acknowledgement
    coordinator ! Coordinator.AddTask(this)
    promise.future
  }
  
  def nextPossibleStart(currentTime:Int, resourceMap:Map[String,TaskResource]) = {
    (currentTime /: resources){ case (i,rN) => resourceMap.get(rN) match {
      case None => throw new RuntimeException(s"Resource $rN not found!")
      case Some(r) => Math.max(i,r.nextAvailableTimestamp(currentTime))
    }}
  }
  
  def compare(that:Task) = {
    lazy val cPriority = that.priority.compare(this.priority)
    lazy val cResources = that.resources.size.compare(this.resources.size)
    lazy val cAge = 
      if (this.createdTime < 0 && that.createdTime < 0) 0 
      else if (this.createdTime < 0) 1
      else if (that.createdTime < 0) -1
      else this.createdTime.compare(that.createdTime)
    lazy val cDuration = that.duration.estimate.compare(this.duration.estimate)
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

case class TaskGenerator (name :String, simulation:String, duration:ValueGenerator[Int]=new ConstantGenerator(1), val cost:ValueGenerator[Int]=new ConstantGenerator(1), interrupt:Int=(-1), priority:Task.Priority=Task.Medium) {
  def create[T](result:T, resources:String*) = new Task(name,simulation,resources,result,duration,cost,interrupt,priority)
  def withPriority(p:Task.Priority) = copy(priority = p)
  def withInterrupt(int:Int) = copy(interrupt = int)
  def withDuration(dur:ValueGenerator[Int]) = copy(duration = dur)
}
