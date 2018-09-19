package com.workflowfm.pew.simulation

import akka.actor._
import akka.util.Timeout
import akka.pattern.ask
import scala.concurrent.duration._
import com.workflowfm.pew.execution.AkkaExecutor
import com.workflowfm.pew.execution.FutureExecutor
import scala.collection.mutable.PriorityQueue


object Coordinator {
  case object Start
  //TODO case object Stop
  case class AddSim(t:Int,sim:Simulation,exe:FutureExecutor)
  case class AddRes(r:TaskResource)
  case class SimDone(name:String,result:String)
  case object Tick
  case object Tack
  case object Tock
  case class AddTask(t:Task)
  case object AckTask
  case class Done(time:Int,metrics:MetricAggregator)

  def props(scheduler :Scheduler, resources :Seq[TaskResource], timeoutMillis:Int = 40)(implicit system: ActorSystem): Props = Props(new Coordinator(scheduler,resources,timeoutMillis)(system))//.withDispatcher("akka.my-dispatcher")
}

class Coordinator(scheduler :Scheduler, val resources :Seq[TaskResource], timeoutMillis:Int)(implicit system: ActorSystem) extends Actor {
  import scala.collection.mutable.Queue
  
  sealed trait Event extends Ordered[Event] { 
    def time:Int
    def compare(that:Event) = {
      that.time.compare(time)
    }
  }
  case class FinishingTask(override val time:Int,task:Task) extends Event
  case class StartingSim(override val time:Int,simulation:Simulation,executor:FutureExecutor) extends Event
  
  var resourceMap :Map[String,TaskResource] = (Map[String,TaskResource]() /: resources){ case (m,r) => m + (r.name -> r)}
  var simulations :Queue[(String,SimulationMetricTracker,FutureExecutor)] = Queue()
  val tasks :Queue[Task] = Queue()
  
  val events = new PriorityQueue[Event]()
    
  var starter:Option[ActorRef] = None
  var time = 1
  
  val metrics = new MetricAggregator()
  
  implicit val timeout = Timeout(timeoutMillis.millis)
  
  def addResource(r:TaskResource) = if (!resourceMap.contains(r.name)) {
    println("["+time+"] Adding resource " + r.name)
    resourceMap += r.name -> r
    //r.idle(time)
  }
  
  protected def startSimulation(t:Int, s:Simulation, e:FutureExecutor) :Boolean = {
    if (t == time) {
      println("["+time+"] Starting simulation: \"" + s.name +"\".") 
      val metrics = new SimulationMetricTracker().simStart(time)
      // change to ? to require acknowledgement
      system.actorOf(SimulationActor.props(s)) ! SimulationActor.Run(self,e) // ,"SimulationActor:" + s.name // TODO need to escape actor name
      simulations += ((s.name,metrics,e))
      true
    } else false
  }
  
  protected def updateSimulation(s:String, f:SimulationMetricTracker=>SimulationMetricTracker) = simulations = simulations map { 
    case (n,m,e) if n.equals(s) => (n,f(m),e) 
    case x => x
  }
  
  def resourceAssign(r:TaskResource) = 
    if (r.isIdle) scheduler.getNextTask(r.name,time,resourceMap,tasks) match {
    case None => 
    case Some(task) => {
      tasks.dequeueFirst (_.compare(task) == 0) // TODO This should remove the correct task right?
      task.taskStart(time)
      val duration = task.duration.get
      (task.resources map (resourceMap.get(_)) flatten) map (_.startTask(task, time, duration))
      events += FinishingTask(time+duration,task)
    }
  }
  
  protected def resourceUpdate(r:TaskResource) :Boolean = {
    r.finishTask(time) match {
      case None => false
      case Some(t) => {
        if (!t.metrics.counted) {
          val res = t.resources.map(resourceMap.get(_)).flatten
          t.taskDone(t,time,t.cost,(0 /: res)(_+_.costPerTick))
          res map {r=>r.taskDone(t.metrics,r.costPerTick)}
          updateSimulation(t.simulation,_.taskDone(t.metrics))
          metrics += t
        }
        true
      }
    }
  }
  
  protected def workflowsReady = simulations forall (_._3.simulationReady)
  
  protected def tick :Unit = {
    if (!events.isEmpty) { 
      val event = events.dequeue()
      if (event.time < time) {
         println("["+time+"] *** Unable to handle past event for time: ["+event.time+"]")
      } else {
    	    time = event.time
    			println("["+time+"] ========= Event! ========= ")

    			event match {
    			  case FinishingTask(time,task) => resourceMap map { case (n,r) => (n,resourceUpdate(r)) }
    			  case StartingSim(time,sim,exec) => startSimulation(time,sim,exec)
    	    }
      }
    }
//    val startedActors = res filter (_._1 == true) map (_._2.executor)
//    for (a <- startedActors)
//      a ? AkkaExecutor.Ping 
    
    for (i <- 1 to 100 if !workflowsReady) {
      println("["+time+"] Waiting for workflow progress...")
      Thread.sleep(timeoutMillis)
    }
      
//    val simActors = simulations map (_._2.executor)
//    for (a <- simActors)
//      a ? AkkaExecutor.Ping
    self ! Coordinator.Tack
  }
  
protected def tack :Unit = {
    resourceMap map { case (n,r) => (n,resourceAssign(r)) }
    if (events.isEmpty && tasks.isEmpty && workflowsReady) { //&& resources.forall(_.isIdle)
      println("["+time+"] All events done. All tasks done. All workflows idle. All resources idle.")
      resourceMap.values map (metrics += _)
      starter map { a => a ! Coordinator.Done(time,metrics) }
    } else {
      //Thread.sleep(halfTickMillis)
      self ! Coordinator.Tick
    }
  }
  
  def start(a:ActorRef) = if (starter.isEmpty) { starter = Some(a) ; tick }

  def receive = {
    case Coordinator.AddSim(t,s,e) =>
      events += StartingSim(t,s,e)
    case Coordinator.AddRes(r) => addResource(r)
    case Coordinator.SimDone(name,result) => {
      simulations.dequeueFirst(_._1.equals(name)) map { x => metrics += (x._1,x._2.setResult(result).simDone(time).metrics) }
      println("["+time+"] Simulation " + name + " reported done.")
    }
    
    case Coordinator.AddTask(t) => {
      println("["+time+"] Adding task " + t.name + " ("+t.simulation+").")
      t.created(time)
      tasks += t
      //sender() ! Coordinator.AckTask //uncomment this to acknowledge AddTask
    }

    case Coordinator.Start => start(sender)
    case Coordinator.Tick => tick
    case Coordinator.Tack => tack
    //case Coordinator.Tock => tock
      
    case SimulationActor.AckRun => Unit
  }
}