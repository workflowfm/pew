package com.workflowfm.pew.simulation

import akka.actor._
import akka.util.Timeout
import akka.pattern.ask
import scala.concurrent.duration._
import com.workflowfm.pew.metrics._
import com.workflowfm.pew.execution._
import scala.collection.mutable.PriorityQueue



object Coordinator {
  case object Start
  //TODO case object Stop
  case class Done(time:Int,metrics:MetricAggregator)
  
  case class AddSim(t:Int,sim:Simulation,exe:SimulationExecutor)
  case class AddSims(l:Seq[(Int,Simulation)],exe:SimulationExecutor)
  case class AddSimNow(sim:Simulation,exe:SimulationExecutor)
  case class AddSimsNow(l:Seq[Simulation],exe:SimulationExecutor)
  
  case class AddRes(r:TaskResource)
  case class SimDone(name:String,result:String)

  case class AddTask(t:Task)
  case object AckTask

  case object Tick
  case object Tack

  def props(scheduler :Scheduler, resources :Seq[TaskResource], timeoutMillis:Int = 40)(implicit system: ActorSystem): Props = Props(new Coordinator(scheduler,resources,timeoutMillis)(system))//.withDispatcher("akka.my-dispatcher")
}

class Coordinator(scheduler :Scheduler, resources :Seq[TaskResource], timeoutMillis:Int)(implicit system: ActorSystem) extends Actor {
  import scala.collection.mutable.Queue
  
  sealed trait Event extends Ordered[Event] { 
    def time:Int
    def compare(that:Event) = {
      that.time.compare(time)
    }
  }
  case class FinishingTask(override val time:Int,task:Task) extends Event
  case class StartingSim(override val time:Int,simulation:Simulation,executor:SimulationExecutor) extends Event
  
  var resourceMap :Map[String,TaskResource] = (Map[String,TaskResource]() /: resources){ case (m,r) => m + (r.name -> r)}
  var simulations :Queue[(String,WorkflowMetricTracker,SimulationExecutor)] = Queue()
  val tasks :Queue[Task] = Queue()
  
  val events = new PriorityQueue[Event]()
    
  var starter:Option[ActorRef] = None
  var time = 1
  
  val metrics = new MetricAggregator()
  
  implicit val timeout = Timeout(timeoutMillis.millis)
  
  def addResource(r:TaskResource) = if (!resourceMap.contains(r.name)) {
    println("["+time+"] Adding resource " + r.name)
    resourceMap += r.name -> r
  }
  
  protected def startSimulation(t:Int, s:Simulation, e:SimulationExecutor) :Boolean = {
    if (t == time) {
      println("["+time+"] Starting simulation: \"" + s.name +"\".") 
      val metrics = new WorkflowMetricTracker().simStart(time)
      // change to ? to require acknowledgement
      system.actorOf(SimulationActor.props(s)) ! SimulationActor.Run(self,e) // TODO need to escape actor name
      simulations += ((s.name,metrics,e))
      true
    } else false
  }
  
  protected def updateSimulation(s:String, f:WorkflowMetricTracker=>WorkflowMetricTracker) = simulations = simulations map { 
    case (n,m,e) if n.equals(s) => (n,f(m),e) 
    case x => x
  }
  
  def resourceAssign(r:TaskResource) = 
    if (r.isIdle) scheduler.getNextTask(r.name,time,resourceMap,tasks) match {
    case None => 
    case Some(task) => {
      tasks.dequeueFirst (_.compare(task) == 0) // TODO This should remove the correct task right?
      startTask(task)
    }
  }
  
  protected def startTask(task:Task) {
    task.taskStart(time)
    val duration = task.duration.get
    (task.resources map (resourceMap.get(_)) flatten) map (_.startTask(task, time, duration))
    events += FinishingTask(time+duration,task)
  }
  
  protected def runNoResourceTasks() {
    tasks.dequeueAll(_.resources.isEmpty).map(startTask)
  }
   
  protected def resourceUpdate(r:TaskResource) :Boolean = {
    r.finishTask(time) match {
      case None => false
      case Some(t) => {
        val res = t.resources.map(resourceMap.get(_)).flatten
        res map {r=>r.taskDone(t.metrics,r.costPerTick)}
        true
      }
    }
  }
  
  protected def finishTask(task:Task) {
    val res = task.resources.map(resourceMap.get(_)).flatten
    task.execute(time)
    task.taskDone(task,time,task.cost,(0 /: res)(_+_.costPerTick))
    updateSimulation(task.simulation,_.taskDone(task.metrics))
    metrics += task
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
    			  case FinishingTask(_,task) => {
    			    resourceMap map { case (n,r) => (n,resourceUpdate(r)) }
    			    finishTask(task)
    			  }
    			  case StartingSim(_,sim,exec) => startSimulation(time,sim,exec)
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
    
    self ! Coordinator.Tack // We need to give workflows a chance to generate new tasks
  }
  
protected def tack :Unit = {
    resourceMap map { case (n,r) => (n,resourceAssign(r)) }
    runNoResourceTasks()
    
    if (events.isEmpty && tasks.isEmpty && workflowsReady) { //&& resources.forall(_.isIdle)
      println("["+time+"] All events done. All tasks done. All workflows idle. All resources idle.")
      resourceMap.values map (metrics += _)
      starter map { a => a ! Coordinator.Done(time,metrics) }
      
    } else {
      self ! Coordinator.Tick
    }
  }
  
  def start(a:ActorRef) = if (starter.isEmpty) { starter = Some(a) ; tick }

  def receive = {
    case Coordinator.AddSim(t,s,e) =>
      events += StartingSim(t,s,e)
    case Coordinator.AddSims(l,e) =>
      events ++= l map { case (t,s) => StartingSim(t,s,e) }
    case Coordinator.AddSimNow(s,e) =>
      events += StartingSim(time,s,e)
    case Coordinator.AddSimsNow(l,e) =>
      events ++= l map { s => StartingSim(time,s,e) }  
      
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
      
    case SimulationActor.AckRun => Unit
  }
}