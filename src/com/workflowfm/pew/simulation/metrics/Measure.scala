package com.workflowfm.pew.simulation.metrics

import com.workflowfm.pew.simulation._

/** Metrics for a simulated [[Task]] that consumed virtual time.
  *
  * @param id the unique ID of the [[Task]]
  * @param task the name of the [[Task]]
  * @param simulation the name of the simulation the [[Task]] belongs to
  * @param created the virtual timestamp when the [[Task]] was created and entered the [[Coordinator]]
  * @param started the virtual timestamp when the [[Task]] started executing, or [[scala.None]] if it has not started yet
  * @param duration the virtual duration of the [[Task]]
  * @param cost the cost associated with the [[Task]]
  * @param resources the list of names of the [[TaskResource]]s this [[Task]] used
  */
case class TaskMetrics (
    id:Long, 
    task:String, 
    simulation:String, 
    created:Long, 
    started:Option[Long], 
    duration:Long, 
    cost:Long, 
    resources:Seq[String]
      ) {
  /** Sets the starting time for the [[Task]]. */
  def start(st:Long) = copy(started=Some(st))

  /** Calculates the task delay as the difference of the creation and starting times. */
  def delay = started match {
    case None => 0L
    case Some(s) => s - created
  }

  /** Returns the full task and simulation name. */
  def fullName = s"$task($simulation)"
}
object TaskMetrics {
  /** Generates [[TaskMetrics]] from a given [[Task]] assuming it has not started yet. */
  def apply(task:Task):TaskMetrics = TaskMetrics(task.id, task.name, task.simulation, task.created, None, task.duration, task.cost, task.resources)
}


/** Metrics for a [[Simulation]] that has already started.
  *
  * @param name the unique name of the [[Simulation]]
  * @param started the virtual timestamp when the [[Simulation]] started executing
  * @param duration the virtual duration of the [[Simulation]]
  * @param delay the sum of all delays for all involved [[Task]]s
  * @param tasks the number of [[Task]]s associated with the [[Simulation]] so far
  * @param cost the total cost associated with the [[Simulation]] so far
  * @param result a `String` representation of the returned result from the [[Simulation]], or [[scala.None]] if it still running. In case of failure, the field is populated with the localized message of the exception thrown 
  */
case class SimulationMetrics(
    name:String, 
    started:Long, 
    duration:Long, 
    delay:Long,
    tasks:Int, 
    cost:Long, 
    result:Option[String]
      ) {
  /** Adds some time to the total duration. */
  def addDuration(d:Long) = copy(duration = duration + d)
  /** Adds some cost to the total cost. */
  def addCost(c:Long) = copy(cost = cost + c)
  /** Adds some delay to the total delay. */
  def addDelay(d:Long) = copy(delay = delay + d)
  /** Updates the metrics given a new [[Task]] that is created as part of the [[Simulation]]. */
  def task(task:Task) = copy(tasks = tasks + 1, cost = cost + task.cost)
  /** Updates the metrics given that the [[Simulation]] has completed with a certain result.
    * @param res the result of the [[Simulation]] or localized message of the exception in case of failure
    * @param time the virtual timestamp when the [[Simulation]] finished
    */
  def done(res:String, time:Long) = copy( result = Some(res), duration = duration + time - started )
}
object SimulationMetrics {
  /** Initialize metrics for a named [[Simulation]] starting at the given virtual time. */
  def apply(name:String, t:Long):SimulationMetrics = SimulationMetrics(name,t,0L,0L,0,0L,None) 
}


/** Metrics for at [[TaskResource]].
  *
  * @param name the unique name of the [[TaskResource]]
  * @param busyTime the total amount of virtual time that the [[TaskResource]] has been busy, i.e. attached to a [[Task]]
  * @param idleTime the total amount of virtual time that the [[TaskResource]] has been idle, i.e. not attached to any [[Task]]
  * @param tasks the number of different [[Task]]s that have been attached to this [[TaskResource]]
  * @param cost the total cost associated with this [[TaskResource]]
  */
case class ResourceMetrics (
    name:String, 
    busyTime:Long, 
    idleTime:Long, 
    tasks:Int, 
    cost:Long
      ) {
  /** Adds some idle time to the total. */
  def idle(i:Long) = copy(idleTime = idleTime + i)
  /** Updates the metrics given a new [[Task]] has been attached to the [[TaskResource]]. */
  def task(task:Task, costPerTick:Long) = copy(
      tasks = tasks + 1, 
      cost = cost + task.duration * costPerTick, 
      busyTime = busyTime + task.duration
  )  
}
object ResourceMetrics {
  /** Initialize metrics given the name of a [[TaskResource]]. */
  def apply(name:String):ResourceMetrics = ResourceMetrics(name,0L,0L,0,0L)
  /** Ihitialize metrics given a [[TaskResource]]. */
  def apply(r:TaskResource):ResourceMetrics = ResourceMetrics(r.name,0L,0L,0,0L) 
}


/** Collects/aggregates metrics across multiple tasks, resources, and simulations. */
class SimMetricsAggregator {
  import scala.collection.immutable.Map

  /** The '''real''' (system) time that measurement started, or [[scala.None]] if it has not started yet. */
  var start:Option[Long] = None
  /** The '''real''' (system) time that measurement finished, or [[scala.None]] if it has not finished yet. */
  var end:Option[Long] = None

  /** Marks the start of metrics measurement with the current system time. */
  def started = start match {
    case None => start = Some(System.currentTimeMillis())
    case _ => ()
  }

  /** Marks the end of metrics measurement with the current system time. */
  def ended = end = Some(System.currentTimeMillis())

  /** Task metrics indexed by task ID. */
  val taskMap = scala.collection.mutable.Map[Long,TaskMetrics]()
  /** Simulation metrics indexed by name. */
  val simMap = scala.collection.mutable.Map[String,SimulationMetrics]()
  /** Resource metrics indexed by name. */
  val resourceMap = scala.collection.mutable.Map[String,ResourceMetrics]()

  
  // Set 

  /** Adds a new [[TaskMetrics]] instance, taking care of indexing automatically
    * Overwrites a previous instance with the same IDs
    */
  def +=(m:TaskMetrics):TaskMetrics = { taskMap += (m.id->m) ; m }
  /** Adds a new [[SimulationMetrics]] instance, taking care of indexing automatically
    * Overwrites a previous instance with the same IDs
    */
  def +=(m:SimulationMetrics):SimulationMetrics = { simMap += (m.name->m) ; m }
  /** Adds a new [[ResourceMetrics]] instance, taking care of indexing automatically
    * Overwrites a previous instance with the same IDs
    */
  def +=(m:ResourceMetrics):ResourceMetrics = { resourceMap += (m.name->m) ; m }

  /** Initializes and adds a new [[TaskMetrics]] instance given a new [[Task]]. */
  def +=(task:Task):TaskMetrics = this += TaskMetrics(task)
  /** Initializes and adds a new [[SimulationMetrics]] instance given the name of the simulation starting now
    * and the current virtual time.
    */
  def +=(s:Simulation,t:Long):SimulationMetrics = this += SimulationMetrics(s.name,t)
  /** Initializes and adds a new [[ResourceMetrics]] instance given a new [[TaskResource]]. */
  def +=(r:TaskResource):ResourceMetrics = this += ResourceMetrics(r)

  
  // Update
  /** Updates a [[TaskMetrics]] instance.
    *
    * @return the updated [[taskMap]] or [[scala.None]] if the identified instance does not exist
    *
    * @param taskID the task ID for the involved [[Task]]
    * @param u a function to update the [[TaskMetrics]] instance
    *
    * @see [[com.workflowfm.pew.metrics.MetricsAggregator]] for examples in a similar context
    */  
  def ^(taskID:Long)(u:TaskMetrics=>TaskMetrics):Option[TaskMetrics] = 
    taskMap.get(taskID).map { m => this += u(m) }

  /** Updates a [[TaskMetrics]] instance.
    *
    * @return the updated [[taskMap]] or [[scala.None]] if the identified instance does not exist
    *
    * @param task the involved [[Task]]
    * @param u a function to update the [[TaskMetrics]] instance
    *
    * @see [[com.workflowfm.pew.metrics.MetricsAggregator]] for examples in a similar context
    * 
    */ 
  def ^(task:Task)(u:TaskMetrics=>TaskMetrics):Option[TaskMetrics] = 
    taskMap.get(task.id).map { m => this += u(m) }

  /** Updates a [[SimulationMetrics]] instance.
    *
    * @return the updated [[simMap]] or [[scala.None]] if the identified instance does not exist
    *
    * @param simulation the involved [[Simulation]]
    * @param u a function to update the [[SimulationMetrics]] instance
    *
    * @see [[com.workflowfm.pew.metrics.MetricsAggregator]] for examples in a similar context
    * 
    */
  def ^(simulation:Simulation)(u:SimulationMetrics=>SimulationMetrics):Option[SimulationMetrics] =
    simMap.get(simulation.name).map { m => this += u(m) }

  /** Updates a [[SimulationMetrics]] instance.
    *
    * @return the updated [[simMap]] or [[scala.None]] if the identified instance does not exist
    *
    * @param simulationName the name of the involved [[Simulation]]
    * @param u a function to update the [[SimulationMetrics]] instance
    *
    * @see [[com.workflowfm.pew.metrics.MetricsAggregator]] for examples in a similar context
    * 
    */
  def ^(simulationName:String)(u:SimulationMetrics=>SimulationMetrics):Option[SimulationMetrics] = 
    simMap.get(simulationName).map { m => this += u(m) }

  /** Updates a [[ResourceMetrics]] instance.
    *
    * @return the updated [[resourceMap]] or [[scala.None]] if the identified instance does not exist
    *
    * @param resource the involved [[TaskResource]]
    * @param u a function to update the [[ResourceMetrics]] instance
    *
    * @see [[com.workflowfm.pew.metrics.MetricsAggregator]] for examples in a similar context
    * 
    */
  def ^(resource:TaskResource)(u:ResourceMetrics=>ResourceMetrics):Option[ResourceMetrics] = 
    resourceMap.get(resource.name).map { m => this += u(m) }
  
  
  // Getters

  /** Returns all the tracked instances of [[TaskMetrics]] sorted by starting time. */
  def taskMetrics = taskMap.values.toSeq.sortBy(_.started)
  /** Returns all the tracked instances of [[SimulationMetrics]] sorted by simulation name. */
  def simulationMetrics = simMap.values.toSeq.sortBy(_.name)
  /** Returns all the tracked instances of [[ResourceMetrics]] sorted by resource time. */
  def resourceMetrics = resourceMap.values.toSeq.sortBy(_.name)
  /** Returns a [[scala.collection.immutable.Set]] of all task names being tracked.
    * This is useful when using task names as a category, for example to colour code tasks in the timeline.
    */
  def taskSet = taskMap.values.map(_.task).toSet[String]

  /** Returns all the tracked instances of [[TaskMetrics]] associated with a particular [[TaskResource]], sorted by starting time.
    * @param r the tracked [[ResourceMetrics]] of the resource
    */
  // TODO: we used to have 2 levels of sorting!
  def taskMetricsOf(r:ResourceMetrics) = taskMap.values.toSeq.filter(_.resources.contains(r.name)).sortBy(_.started)
    /** Returns all the tracked instances of [[TaskMetrics]] associated with a particular [[Simulation]], sorted by starting time.
    * @param r the tracked [[SimulationMetrics]] of the resource
    */
  def taskMetricsOf(s:SimulationMetrics) = taskMap.values.toSeq.filter(_.simulation.equals(s.name)).sortBy(_.started)
}
