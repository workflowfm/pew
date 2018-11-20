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

  def props(scheduler: Scheduler, startingTime: Long = 0L, timeoutMillis: Int = 50)(implicit system: ActorSystem): Props = Props(new Coordinator(scheduler,startingTime,timeoutMillis)(system))//.withDispatcher("akka.my-dispatcher")
}

class Coordinator(scheduler :Scheduler, startingTime:Long, timeoutMillis:Int)(implicit system: ActorSystem) extends Actor {
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
  var time = startingTime
  var taskID = 0L
  
  val metrics = new SimMetricsAggregator()
  
  implicit val timeout = Timeout(timeoutMillis.millis)
  
  def addResource(r:TaskResource) = if (!resourceMap.contains(r.name)) {
    println("["+time+"] Adding resource " + r.name)
    resourceMap += r.name -> r
    metrics += r
  }

  protected def handleEvent(event:Event) = event match {
    //println("["+time+"] ========= Event! ========= ")
    // A task is finished
    case FinishingTask(t,task) if (t == time) => {
      // Unbind the resources
      //resourceMap map { case (n,r) => (n,resourceUpdate(r)) } TODO why was that better originally?
      task.taskResources(resourceMap).foreach(_.finishTask(time))
      // Mark the task as completed
      // This will cause workflows to reduce and maybe produce more tasks
      task.complete(time)
    }
    // A simulation (workflow) is starting now
    case StartingSim(t,sim,exec) if (t == time)=> startSimulation(time,sim,exec)
    case _ => println(s"[$time] <*> <*> <*> Failed to handle event: $event")
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
  
  protected def resourceIdle(r:TaskResource) = if (r.isIdle) {
    (metrics^r)(_.idle(time-r.lastUpdate))
    r.update(time)
  }

  protected def startTask(task:Task) {
    tasks.dequeueFirst (_.id == task.id)
    // Mark the start of the task in the metrics
    (metrics^task.id)(_.start(time))
    task.taskResources(resourceMap) map { r =>
      // Update idle time if resource has been idle
      if (r.isIdle) (metrics^r)(_.idle(time-r.lastUpdate))
      // Bind each resource to this task
      r.startTask(task, time)
      // Add the task and resource cost to the resource metrics
      (metrics^r)(_.task(task, r.costPerTick))
    }
    // Add the task to the simulation metrics
    (metrics^task.simulation)(_.task(task).addDelay(time - task.created))
    // Generate a FinishTask event to be triggered at the end of the event
    events += FinishingTask(time+task.duration,task)
  }
  
  /**
    * Runs all tasks that require no resources
    */
  protected def runNoResourceTasks() = tasks.dequeueAll(_.resources.isEmpty).map(startTask)
  
  protected def workflowsReady = simulations forall (_._2.simulationReady)
  
  /**
    * First half of a clock tick
    * We need two halves because we want to give the workflows a chance to reduce
    * and register new tasks to the Coordinator. The Coordinator must receive those
    * through messages between now and the Tack message.  
    */
  protected def tick :Unit = {
    // Are events pending?
    if (!events.isEmpty) {
      // Grab the first event
      val event = events.dequeue()
      // Did we somehow go past the event time? This should never happen.
      if (event.time < time) {
        println("["+time+"] *** Unable to handle past event for time: ["+event.time+"]")
      } else {
        // Jump ahead to the event time. This is a priority queue so we shouldn't skip any events
    	time = event.time
        handleEvent(event)

        while (events.headOption.map(_.time == time).getOrElse(false))
          handleEvent(events.dequeue)
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
    // Make sure we run tasks that need no resources
    runNoResourceTasks()
    // Assign the next tasks
    scheduler.getNextTasks(tasks, time, resourceMap).foreach(startTask)
    // Update idle resources
    resourceMap.values.foreach(resourceIdle)

    
    // We finish if there are no events, no tasks, and all workflows have reduced
    // Actually all workflows must have reduced at this stage, but we check anyway
    if (events.isEmpty && tasks.isEmpty && workflowsReady) { //&& resources.forall(_.isIdle)
      println("["+time+"] All events done. All tasks done. All workflows idle. All resources idle.")
      // Tell whoever started us that we are done
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
      // Create the task
      val t = gen.create(taskID,time,resources:_*)
      // This is ok only if the simulation is running in memory
      // Promises cannot be sent over messages otherwise as they are not serializable
      promise.completeWith(t.promise.future)
      // Make sure the next taskID will be fresh
      taskID = taskID + 1L
      println(s"[$time] Adding task [$taskID]: ${t.name} (${t.simulation}).")
      
      // Calculate the cost of all resource usage. We only know this now!
      val resourceCost = (0L /: t.taskResources(resourceMap)) { case (c,r) => c + r.costPerTick * t.duration }
      t.addCost(resourceCost)
      
      metrics += t
      tasks += t
      
      //sender() ! Coordinator.AckTask(t) //uncomment this to acknowledge AddTask
    }

    case Coordinator.Start => start(sender)
    case Coordinator.Tick => tick
    case Coordinator.Tack => tack
      
    case SimulationActor.AckRun => Unit
  }
}
