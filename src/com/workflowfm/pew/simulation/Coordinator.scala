package com.workflowfm.pew.simulation

import akka.actor._
import akka.util.Timeout
import akka.pattern.ask
import scala.concurrent.duration._
import com.workflowfm.pew.execution.AkkaExecutor
import com.workflowfm.pew.execution.FutureExecutor


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

  def props(scheduler :Scheduler, handler :MetricsOutput, resources :Seq[TaskResource], halfTickMillis:Int = 40)(implicit system: ActorSystem): Props = Props(new Coordinator(scheduler,handler,resources,halfTickMillis)(system))//.withDispatcher("akka.my-dispatcher")
}
class Coordinator(scheduler :Scheduler, handler :MetricsOutput, var resources :Seq[TaskResource], halfTickMillis:Int)(implicit system: ActorSystem) extends Actor {
  import scala.collection.mutable.Queue
  
  var resourceMap :Map[String,TaskResource] = (Map[String,TaskResource]() /: resources){ case (m,r) => m + (r.name -> r)}
  val simulations :Queue[(Int,Simulation,FutureExecutor)] = Queue()
  val queue = new Queue[Task]()
    
  var started = false
  var ticks = 0
  var simulationsDone = 0
  
  val metrics = new MetricAggregator()
  
  implicit val timeout = Timeout(halfTickMillis.millis)
  
  def addResource(r:TaskResource) = if (!resourceMap.contains(r.name)) {
    println("["+ticks+"] Adding resource " + r.name)
    resources = r +: resources
    resourceMap += r.name -> r
    1 to ticks foreach {_ => r.idleTick() }
  }
  
  protected def startSimulation(t:Int)(ts:Int,s:Simulation,e:FutureExecutor) :Boolean = {
    if (t == ts) {
      println("["+ticks+"] Starting simulation: \"" + s.name +"\".") 
      s.simStart(ticks)
      // change to ? to require acknowledgement
      system.actorOf(SimulationActor.props(s)) ! SimulationActor.Run(self,e) // ,"SimulationActor:" + s.name // TODO need to escape actor name
      true
    } else false
  }
  
  def resourceIdle(r:TaskResource) = 
    if (r.isIdle) scheduler.getNextTask(r.name,ticks,resourceMap,queue) match {
    case None => {
      r.idleTick()
    }
    case Some(task) => {
      queue.dequeueFirst (_.compare(task) == 0) // TODO This should remove the correct task right?
      task.taskStart(ticks)
      (task.resources map (resourceMap.get(_)) flatten) map (_.startTask(task, ticks))
    }
  }
  
  protected def resourceTick(r:TaskResource) :TaskResource.State = {
    r.tick(ticks) match {
      case TaskResource.Idle => TaskResource.Idle
      case TaskResource.Busy => TaskResource.Busy
      case TaskResource.Finished(t) => {
        if (!t.metrics.counted) {
          val res = t.resources.map(resourceMap.get(_)).flatten
          t.taskDone(t,ticks,t.cost,(0 /: res)(_+_.costPerTick))
          res map {r=>r.taskDone(t.metrics,r.costPerTick)}
          simulations.find(_._2.name == t.simulation) map (_._2.taskDone(t.metrics))
          metrics += t
        }
        TaskResource.Idle
      }
    }
  }
  
  protected def workflowsReady = simulations forall (_._3.simulationReady)
  
  protected def tick :Unit = {
    ticks += 1
    println("["+ticks+"] ========= Tick! ========= ")
    //val res = simulations map {case (t,s) => startSimulation(ticks)(t,s)}
    val res = simulations map {case (t,s,e) => (startSimulation(ticks)(t,s,e),s)}
    //if (res.exists(_._1 == true)) 
    //  Thread.sleep(halfTickMillis)
    
//    val startedActors = res filter (_._1 == true) map (_._2.executor)
//    for (a <- startedActors)
//      a ? AkkaExecutor.Ping
    self ! Coordinator.Tack
  }
  
  protected def tack :Unit = {
    resources map (resourceTick(_))
    
    for (i <- 1 to 100 if !workflowsReady) {
      println("["+ticks+"] Waiting for workflow progress...")
      Thread.sleep(halfTickMillis)
    }
      
//    val simActors = simulations map (_._2.executor)
//    for (a <- simActors)
//      a ? AkkaExecutor.Ping
    self ! Coordinator.Tock
  }
  
  protected def tock :Unit = {
    resources map (resourceIdle(_))
    if (simulationsDone >= simulations.size && queue.isEmpty && resources.forall(_.isIdle)) {
      println("["+ticks+"] All simulations done. Queue empty. All resources idle.")
      resources map (metrics += _)
      system.terminate() // TODO consider not doing this here
      handler(ticks,metrics)
    } else {
      //Thread.sleep(halfTickMillis)
      self ! Coordinator.Tick
    }
  }
  
  def start = if (!started) { started = true ; tick }

  def receive = {
    case Coordinator.AddSim(t,s,e) =>
      simulations += ((t,s,e))
    case Coordinator.AddRes(r) => addResource(r)
    case Coordinator.SimDone(s) => {
      simulationsDone += 1
      s.simDone(ticks)
      metrics += s
      println("["+ticks+"] Simulation " + s.name + " reported done.")
    }
    
    case Coordinator.AddTask(t) => {
      println("["+ticks+"] Adding task " + t.name + " ("+t.simulation+").")
      t.created(ticks)
      queue += t
      //sender() ! Coordinator.AckTask //uncomment this to acknowledge AddTask
    }

    case Coordinator.Start => start
    case Coordinator.Tick => tick
    case Coordinator.Tack => tack
    case Coordinator.Tock => tock
      
    case SimulationActor.AckRun => Unit
  }
}