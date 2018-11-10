package com.workflowfm.pew.simulation

import akka.actor._
import akka.util.Timeout
import akka.pattern.ask
import scala.concurrent.duration._
import com.workflowfm.pew.simulation.metrics._
import com.workflowfm.pew.execution._
import scala.collection.mutable.PriorityQueue
import scala.concurrent.Promise



object Coordinator {
  case object Start
  //TODO case object Stop
  case class Done(time:Long,metrics:SimMetricsAggregator)
  
  case class AddSim(t:Long,sim:Simulation,exe:SimulatorExecutor[_])
  case class AddSims(l:Seq[(Long,Simulation)],exe:SimulatorExecutor[_])
  case class AddSimNow(sim:Simulation,exe:SimulatorExecutor[_])
  case class AddSimsNow(l:Seq[Simulation],exe:SimulatorExecutor[_])
  
  case class AddResource(r:TaskResource)
  case class AddResources(l:Seq[TaskResource])
  
  case class SimDone(name:String,result:String)

  case class AddTask(t:TaskGenerator, promise:Promise[Unit], resources:Seq[String])
  case object AckTask

  case object Tick
  case object Tack

  def props(scheduler :Scheduler, timeoutMillis:Int = 40)(implicit system: ActorSystem): Props = Props(new Coordinator(scheduler,timeoutMillis)(system))//.withDispatcher("akka.my-dispatcher")
}

class Coordinator(scheduler :Scheduler, timeoutMillis:Int)(implicit system: ActorSystem) extends Actor {
  import scala.collection.mutable.Queue
  
  sealed trait Event extends Ordered[Event] { 
    def time:Long
    def compare(that:Event) = {
      that.time.compare(time)
    }
  }
  case class FinishingTask(override val time:Long,task:Task) extends Event
  case class StartingSim(override val time:Long,simulation:Simulation,executor:SimulatorExecutor[_]) extends Event
  
  var resourceMap :Map[String,TaskResource] = Map[String,TaskResource]() ///: resources){ case (m,r) => m + (r.name -> r)}
  var simulations :Map[String,SimulatorExecutor[_]] = Map[String,SimulatorExecutor[_]]()
  val tasks :Queue[Task] = Queue()
  
  val events = new PriorityQueue[Event]()
    
  var starter:Option[ActorRef] = None
  var time = 1L
  var taskID = 0L
  
  val metrics = new SimMetricsAggregator()
  
  implicit val timeout = Timeout(timeoutMillis.millis)
  
  def addResource(r:TaskResource) = if (!resourceMap.contains(r.name)) {
    println("["+time+"] Adding resource " + r.name)
    resourceMap += r.name -> r
    metrics += r
  }
  
  protected def startSimulation(t:Long, s:Simulation, e:SimulatorExecutor[_]) :Boolean = {
    if (t == time) {
      println("["+time+"] Starting simulation: \"" + s.name +"\".") 
      metrics += (s,t)
      // change to ? to require acknowledgement
      system.actorOf(SimulationActor.props(s)) ! SimulationActor.Run(self,e) // TODO need to escape actor name
      simulations += (s.name -> e)
      true
    } else false
  }
  
//  protected def updateSimulation(s:String, f:WorkflowMetricTracker=>WorkflowMetricTracker) = simulations = simulations map { 
//    case (n,m,e) if n.equals(s) => (n,f(m),e) 
//    case x => x
//  }
  
  def resourceAssign(r:TaskResource) = 
    if (r.isIdle) scheduler.getNextTask(r.name,time,resourceMap,tasks) match {
    case None => 
    case Some(task) => {
      tasks.dequeueFirst (_.compare(task) == 0) // TODO This should remove the correct task right?
      startTask(task)
    }
  }
  
  protected def startTask(task:Task) {
    (metrics^task.id)(_.start(time))
    task.taskResources(resourceMap) map { r => 
      r.startTask(task, time)
      (metrics^r)(_.task(task, r.costPerTick))
    }
    events += FinishingTask(time+task.duration,task)
    (metrics^task.simulation)(_.task(task))
  }
  
  protected def runNoResourceTasks() {
    tasks.dequeueAll(_.resources.isEmpty).map(startTask)
  }
     
  protected def workflowsReady = simulations forall (_._2.simulationReady)
  
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
    			    //resourceMap map { case (n,r) => (n,resourceUpdate(r)) } TODO why was that better originally?
    			    task.taskResources(resourceMap).foreach(_.finishTask(time))
    			    task.complete(time)
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
    case Coordinator.AddSim(t,s,e) => events += StartingSim(t,s,e)
    case Coordinator.AddSims(l,e) => events ++= l map { case (t,s) => StartingSim(t,s,e) }
    case Coordinator.AddSimNow(s,e) => events += StartingSim(time,s,e)
    case Coordinator.AddSimsNow(l,e) => events ++= l map { s => StartingSim(time,s,e) }  
      
    case Coordinator.AddResource(r) => addResource(r)
    case Coordinator.AddResources(r) => r foreach addResource
    
    case Coordinator.SimDone(name,result) => {
      simulations -= name
      (metrics^name) (_.done(result,time)) 
      println("["+time+"] Simulation " + name + " reported done.")
    }
    
    case Coordinator.AddTask(gen,promise,resources) => {
      val t = gen.create(taskID,resources:_*)
      taskID = taskID + 1L
      println(s"[$time] Adding task [$taskID]: ${t.name} (${t.simulation}).")
           
      val resourceCost = (0L /: t.taskResources(resourceMap)) { case (c,r) => c + r.costPerTick * t.duration }
      t.created(time)
      t.addCost(resourceCost)

      tasks += t
      metrics += t
      //sender() ! Coordinator.AckTask(t) //uncomment this to acknowledge AddTask
      promise.completeWith(t.promise.future)
    }

    case Coordinator.Start => start(sender)
    case Coordinator.Tick => tick
    case Coordinator.Tack => tack
      
    case SimulationActor.AckRun => Unit
  }
}