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
  case class SimDone(s:Simulation)
  case object Tick
  case object Tack
  case object Tock
  case class AddTask(t:Task)
  case object AckTask

  def props(scheduler :Scheduler, handler :MetricsOutput, resources :Seq[TaskResource], timeoutMillis:Int = 40)(implicit system: ActorSystem): Props = Props(new Coordinator(scheduler,handler,resources,timeoutMillis)(system))//.withDispatcher("akka.my-dispatcher")
}

class Coordinator(scheduler :Scheduler, handler :MetricsOutput, var resources :Seq[TaskResource], timeoutMillis:Int)(implicit system: ActorSystem) extends Actor {
  import scala.collection.mutable.Queue
  
  sealed trait Event extends Ordered[Event] { 
    def time:Int
    def compare(that:Event) = {
      time.compare(that.time)
    }
  }
  case class FinishingTask(override val time:Int,task:Task) extends Event
  case class StartingSim(override val time:Int,simulation:Simulation,executor:FutureExecutor) extends Event
  
  
  var resourceMap :Map[String,TaskResource] = (Map[String,TaskResource]() /: resources){ case (m,r) => m + (r.name -> r)}
  //val simulations :Queue[(Int,Simulation,FutureExecutor)] = Queue()
  val simulations :Queue[(Simulation,FutureExecutor)] = Queue()
  val tasks :Queue[Task] = Queue()
  
  val queue = new PriorityQueue[Event]()
    
  var started = false
  var time = 0
  
  val metrics = new MetricAggregator()
  
  implicit val timeout = Timeout(timeoutMillis.millis)
  
  def addResource(r:TaskResource) = if (!resourceMap.contains(r.name)) {
    println("["+time+"] Adding resource " + r.name)
    resources = r +: resources
    resourceMap += r.name -> r
    r.idle(time)
  }
  
  protected def startSimulation(t:Int, s:Simulation, e:FutureExecutor) :Boolean = {
    if (t == time) {
      println("["+time+"] Starting simulation: \"" + s.name +"\".") 
      s.simStart(time)
      // change to ? to require acknowledgement
      system.actorOf(SimulationActor.props(s)) ! SimulationActor.Run(self,e) // ,"SimulationActor:" + s.name // TODO need to escape actor name
      simulations += ((s,e))
      true
    } else false
  }
  
//  def resourceIdle(r:TaskResource) = 
//    if (r.isIdle) scheduler.getNextTask(r.name,ticks,resourceMap,queue) match {
//    case None => {
//      r.idleTick()
//    }
//    case Some(task) => {
//      queue.dequeueFirst (_.compare(task) == 0) // TODO This should remove the correct task right?
//      task.taskStart(ticks)
//      (task.resources map (resourceMap.get(_)) flatten) map (_.startTask(task, ticks))
//    }
//  }
  
//  protected def resourceUpdate(r:TaskResource) :TaskResource.State = {
//    r.tick(ticks) match {
//      case TaskResource.Idle => TaskResource.Idle
//      case TaskResource.Busy => TaskResource.Busy
//      case TaskResource.Finished(t) => {
//        if (!t.metrics.counted) {
//          val res = t.resources.map(resourceMap.get(_)).flatten
//          t.taskDone(t,ticks,t.cost,(0 /: res)(_+_.costPerTick))
//          res map {r=>r.taskDone(t.metrics,r.costPerTick)}
//          simulations.find(_._2.name == t.simulation) map (_._2.taskDone(t.metrics))
//          metrics += t
//        }
//        TaskResource.Idle
//      }
//    }
//  }
  
  protected def workflowsReady = simulations forall (_._2.simulationReady)
  
  protected def tick :Unit = {
    if (!queue.isEmpty) { 
      val event = queue.dequeue()
      if (event.time < time) {
         println("["+time+"] *** Unable to handle past event for time: ["+event.time+"]")
      } else {
    	    time = event.time
    			println("["+time+"] ========= Event! ========= ")

    			event match {
    			  case FinishingTask(time,task) => // TODO resources map (resourceTick(_))
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
    // TODO resources map (resourceIdle(_))
    if (queue.isEmpty && tasks.isEmpty && workflowsReady) { //&& resources.forall(_.isIdle)
      println("["+time+"] Queue empty. All tasks done. All workflows idle. All resources idle.")
      resources map (metrics += _)
      system.terminate() // TODO consider not doing this here
      handler(time,metrics)
    } else {
      //Thread.sleep(halfTickMillis)
      self ! Coordinator.Tick
    }
  }
  
  def start = if (!started) { started = true ; tick }

  def receive = {
    case Coordinator.AddSim(t,s,e) =>
      queue += StartingSim(t,s,e)
    case Coordinator.AddRes(r) => addResource(r)
    case Coordinator.SimDone(s) => {
      s.simDone(time)
      metrics += s
      println("["+time+"] Simulation " + s.name + " reported done.")
    }
    
    case Coordinator.AddTask(t) => {
      println("["+time+"] Adding task " + t.name + " ("+t.simulation+").")
      t.created(time)
      tasks += t
      //sender() ! Coordinator.AckTask //uncomment this to acknowledge AddTask
    }

    case Coordinator.Start => start
    case Coordinator.Tick => tick
    case Coordinator.Tack => tack
    //case Coordinator.Tock => tock
      
    case SimulationActor.AckRun => Unit
  }
}