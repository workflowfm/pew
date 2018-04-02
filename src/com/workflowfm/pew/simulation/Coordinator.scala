package com.workflowfm.pew.simulation

import akka.actor._
import akka.util.Timeout
import akka.pattern.ask
import scala.concurrent.duration._
import com.workflowfm.pew.execution.AkkaExecutor


object Coordinator {
  case object Start
  //TODO case object Stop
  case class AddSim(t:Int,sim:Simulation)
  case class SimDone(s:Simulation)
  case object Tick
  case object Tack
  case object Tock
  case class AddTask(t:Task)
  case object AckTask

  def props(scheduler :Scheduler, handler :MetricsOutput, resources :Seq[TaskResource], halfTickMillis:Int = 10)(implicit system: ActorSystem): Props = Props(new Coordinator(scheduler,handler,resources,halfTickMillis)(system)).withDispatcher("akka.my-dispatcher")
}
class Coordinator(scheduler :Scheduler, handler :MetricsOutput, resources :Seq[TaskResource], halfTickMillis:Int)(implicit system: ActorSystem) extends Actor {
  import scala.collection.mutable.Queue
  
  val resourceMap :Map[String,TaskResource] = (Map[String,TaskResource]() /: resources){ case (m,r) => m + (r.name -> r)}
  val simulations :Queue[(Int,Simulation)] = Queue()
  val queue = new Queue[Task]()
    
  var started = false
  var ticks = 0
  var simulationsDone = 0
  
  val metrics = new MetricAggregator()
  
  implicit val timeout = Timeout(halfTickMillis.millis)
  
  protected def startSimulation(t:Int)(ts:Int,s:Simulation) :Boolean = {
    if (t == ts) {
      println("["+ticks+"] Starting simulation: \"" + s.name +"\".") 
      s.simStart(ticks)
      system.actorOf(SimulationActor.props(s)) ? SimulationActor.Run(self) // ,"SimulationActor:" + s.name // TODO need to escape actor name
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
  
  protected def tick :Unit = {
    ticks += 1
    println("["+ticks+"] ========= Tick! ========= ")
    val res = simulations map {case (t,s) => startSimulation(ticks)(t,s)}
    if (res.exists(_ == true)) 
      Thread.sleep(halfTickMillis)
//    val res = simulations map {case (t,s) => (startSimulation(ticks)(t,s),s)}
//    val startedActors = res filter (_._1 == true) map (_._2.executor)
//    for (a <- startedActors)
//      a ? AkkaExecutor.Ping
    self ! Coordinator.Tack
  }
  
  protected def tack :Unit = {
    resources map (resourceTick(_))
    Thread.sleep(halfTickMillis)
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
    case Coordinator.AddSim(t,s) =>
      simulations += ((t,s))
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
    }

    case Coordinator.Start => start
    case Coordinator.Tick => tick
    case Coordinator.Tack => tack
    case Coordinator.Tock => tock
      
    case SimulationActor.AckRun => Unit
  }
}