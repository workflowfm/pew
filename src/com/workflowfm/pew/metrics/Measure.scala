package com.workflowfm.pew.metrics

import com.workflowfm.pew._
import com.workflowfm.pew.stream.PiEventHandler

/** Metrics for a particular instance of an atomic process.
  *
  * @constructor initialize the metrics of a process for a workflow with ID `piID`, unique call reference `ref`, and process name `process`, starting now
  * 
  * @tparam KeyT the type used for workflow IDs
  * @param piID the ID of the workflow that executed this process
  * @param ref the call reference for that process, i.e. a unique call ID for this particular workflow
  * @param process the name of the process
  * @param start the system time in milliseconds when the process call started
  * @param finish the system time in milliseconds that the process call finished, or [[scala.None]] if it is still running
  * @param result a `String` representation of the returned result from the process call, or [[scala.None]] if it still running. In case of failure, the field is populated with the localized message of the exception thrown 
 */
case class ProcessMetrics[KeyT] (piID:KeyT, ref:Int, process:String, start:Long=System.currentTimeMillis(), finish:Option[Long]=None, result:Option[String]=None) {
  def complete(time:Long,result:Any) = copy(finish=Some(time),result=Some(result.toString))
}

/** Metrics for a particular workflow.
  * 
  * @constructor initialize the metrics of a workflow with ID `piID` starting now
  *
  * @tparam KeyT the type used for workflow IDs
  * @param piID the unique ID of the workflow.
  * @param start the system time in milliseconds when the workflow started executing
  * @param calls the number of calls to atomic processes
  * @param finish the system time in milliseconds that this workflow finished, or [[scala.None]] if it is still running
  * @param result a `String` representation of the returned result from the workflow, or [[scala.None]] if it still running. In case of failure, the field is populated with the localized message of the exception thrown 
 */
case class WorkflowMetrics[KeyT] (piID:KeyT, start:Long=System.currentTimeMillis(), calls:Int=0, finish:Option[Long]=None, result:Option[String]=None) {
  def complete(time:Long,result:Any) = copy(finish=Some(time),result=Some(result.toString))
  def call = copy(calls=calls+1)
}

/** Collects/aggregates metrics across multiple process calls and workflow executions */
class MetricsAggregator[KeyT] {
  import scala.collection.immutable.Map

  /** Process metrics indexed by workflow ID, then by call reference ID */
  val processMap = scala.collection.mutable.Map[KeyT,Map[Int,ProcessMetrics[KeyT]]]()
  /** Workflow metrics indexed by workflow ID */
  val workflowMap = scala.collection.mutable.Map[KeyT,WorkflowMetrics[KeyT]]()
  
  // Set

  /** Adds a new [[ProcessMetrics]] instance, taking care of indexing automatically
    * Overwrites a previous instance with the same IDs
    */
  def +=(m:ProcessMetrics[KeyT]) = {
    val old = processMap.get(m.piID) match {
      case None => Map[Int,ProcessMetrics[KeyT]]()
      case Some(map) => map
    }
    processMap += (m.piID -> (old + (m.ref -> m)))
  }
  /** Adds a new [[WorkflowMetrics]] instance, taking care of indexing automatically
    * Overwrites a previous instance with the same ID
    */
  def +=(m:WorkflowMetrics[KeyT]) = workflowMap += (m.piID->m)
  
  // Update

  /** Updates a [[ProcessMetrics]] instance.
    *
    * @return the updated [[processMap]] or [[scala.None]] if the identified instance does not exist
    *
    * @param piID the workflow ID of the process
    * @param ref the call reference of the process
    * @param u a function to update the [[ProcessMetrics]] instance
    *
    * @example Assume function that updates the name of a process in [[ProcessMetrics]] to `"x"`:
    * {{{ def myUpdate(p: ProcessMetrics[KeyT]): ProcessMetrics[KeyT] = p.copy(process="x") }}}
    * We can update the metrics of process with workflow ID `5` and call reference `2` as follows:
    * {{{ metricsAggregator ^ (5,2,myUpdate) }}}
    * 
    */
  def ^(piID:KeyT,ref:Int,u:ProcessMetrics[KeyT]=>ProcessMetrics[KeyT]) = 
    processMap.get(piID).flatMap(_.get(ref)).map { m => this += u(m) }

  /** Updates a [[WorkflowMetrics]] instance
    *
    * @return the updated [[processMap]] or [[scala.None]] if the identified instance does not exist
    *
    * @param piID the workflow ID of the process
    * @param u a function to update the [[ProcessMetrics]] instance
    *
    * @example Assume function that increases the number of calls [[WorkflowMetrics]] to `"x"`:
    * {{{ def myUpdate(w: WorkflowMetrics[KeyT]): WorkflowMetrics[KeyT] = w.copy(calls=calls+1) }}}
    * We can update the metrics of workflow with ID `5` as follows:
    * {{{ metricsAggregator ^ (5,myUpdate) }}}
    * 
    */
  def ^(piID:KeyT,u:WorkflowMetrics[KeyT]=>WorkflowMetrics[KeyT]) = 
    workflowMap.get(piID).map { m => this += u(m) }

  // Handle events

  /** Handles the event of a workflow starting, updating its metrics accordingly.
    *
    * @param piID the ID of the workflow
    * @param time the timestamp to be recorded as the workflow start
    */
  def workflowStart(piID:KeyT, time:Long=System.currentTimeMillis()):Unit =
    this += WorkflowMetrics(piID,time)

  /** Handles the event of a workflow finishing successfully, updating its metrics accordingly.
    *
    * @param piID the ID of the workflow
    * @param result the return value of the workflow
    * @param time the timestamp to be recorded as the workflow finish
    */
  def workflowResult(piID:KeyT, result:Any, time:Long=System.currentTimeMillis()):Unit =
    this ^ (piID,_.complete(time,result))

  /** Handles the event of a workflow failing with an exception, updating its metrics accordingly.
    *
    * @param piID the ID of the workflow
    * @param ex the thrown exception (or other [[scala.Throwable]])
    * @param time the timestamp to be recorded as the workflow finish
    */
  def workflowException(piID:KeyT, ex:Throwable, time:Long=System.currentTimeMillis()):Unit =
    this ^ (piID,_.complete(time,"Exception: " + ex.getLocalizedMessage))

  /** Handles the event of an atomic process call, updating its metrics accordingly.
    *
    * @param piID the ID of the workflow
    * @param ref the call reference ID
    * @param process the name of the atomic process being called
    * @param time the timestamp to be recorded as the process start
    */
  def procCall(piID:KeyT, ref:Int, process:String, time:Long=System.currentTimeMillis()):Unit = {
    this += ProcessMetrics(piID,ref,process,time)
    this ^ (piID,_.call)
  }

  /** Handles the event of an atomic process returning successfully, updating its metrics accordingly.
    *
    * @param piID the ID of the workflow
    * @param ref the call reference ID
    * @param result the result returned by the process
    * @param time the timestamp to be recorded as the process finish
    */
  def procReturn(piID:KeyT, ref:Int, result:Any, time:Long=System.currentTimeMillis()):Unit =
    this ^ (piID,ref,_.complete(time, result))

  /** Handles the event of an atomic process failing with an exception, updating its metrics accordingly.
    *
    * @param piID the ID of the workflow
    * @param ref the call reference ID
    * @param ex the thrown exception (or other [[scala.Throwable]])
    * @param time the timestamp to be recorded as the process finish
    */
  def processException(piID:KeyT, ref:Int, ex:Throwable, time:Long=System.currentTimeMillis()):Unit =
    processFailure(piID,ref,ex.getLocalizedMessage,time)

  /** Handles the event of an atomic process failing in any way, updating its metrics accordingly.
    *
    * @param piID the ID of the workflow
    * @param ref the call reference ID
    * @param ex a `String` explanation of what went wrong
    * @param time the timestamp to be recorded as the process finish
    */
  def processFailure(piID:KeyT, ref:Int, ex:String, time:Long=System.currentTimeMillis()):Unit = {
    this ^ (piID,_.complete(time,"Exception: " + ex))
    this ^ (piID,ref,_.complete(time,"Exception: " + ex))
  }
  
  // Getters 

  /** Returns the collection of workflow IDs that have been tracked. */
  def keys = workflowMap.keys
  /** Returns all the tracked instances of [[WorkflowMetrics]] sorted by starting time. */
  def workflowMetrics = workflowMap.values.toSeq.sortBy(_.start)
  /** Returns all the tracked instances of [[ProcessMetrics]] sorted by starting time. */
  def processMetrics = processMap.values.flatMap(_.values).toSeq.sortBy(_.start)
  /** Returns all the tracked instances of [[ProcessMetrics]] associated with a particular workflow, sorted by starting time.
    * @param id the ID of the workflow
    */
  def processMetricsOf(id:KeyT) = processMap.getOrElse(id,Map[Int,ProcessMetrics[KeyT]]()).values.toSeq.sortBy(_.start)
  /** Returns a [[scala.collection.immutable.Set]] of all process names being tracked.
    * This is useful when using process names as a category, for example to colour code tasks in the timeline.
    */
  def processSet = processMap.values.flatMap(_.values.map(_.process)).toSet[String]
}

/** A [[MetricsAggregator]] that is also a [[PiEventHandler]].
  * Aggregates metrics automatically based on [[PiEvent]]s and system time.
  *
  * @param name a unique name to identify the instance of [[PiEventHandler]]
  * @param timeFn the [[PiMetadata]] key to retrieve timing information (the recorder system time by default)
  */
class MetricsHandler[KeyT](override val name: String, timeFn: PiMetadata.Key[Long] = PiMetadata.SystemTime )
  extends MetricsAggregator[KeyT] with PiEventHandler[KeyT] {

  /** Converts [[PiEvent]]s to metrics updates. */
  override def apply( e: PiEvent[KeyT] ): Boolean = {
    e match {
      case PiEventStart(i,t)      => workflowStart( i.id, timeFn(t) )
      case PiEventResult(i,r,t)   => workflowResult( i.id, r, timeFn(t) )
      case PiEventCall(i,r,p,_,t) => procCall( i, r, p.iname, timeFn(t) )
      case PiEventReturn(i,r,s,t) => procReturn( i, r, s, timeFn(t) )
      case PiFailureAtomicProcessException(i,r,m,_,t) => processFailure( i, r, m, timeFn(t) )

      case ev: PiFailure[KeyT] =>
        workflowException( ev.id, ev.exception, timeFn(ev.metadata) )
    }
    false
  }
}

