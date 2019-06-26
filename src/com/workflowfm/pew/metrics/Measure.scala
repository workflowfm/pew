package com.workflowfm.pew.metrics

import com.workflowfm.pew._

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
  * @param result a [[String]] representation of the returned result from the process call, or [[scala.None]] if it still running. In case of failure, the field is populated with the `getLocalizedMessage` of the exception thrown 
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
  * @param result a [[String]] representation of the returned result from the workflow, or [[scala.None]] if it still running. In case of failure, the field is populated with the `getLocalizedMessage` of the exception thrown 
 */
case class WorkflowMetrics[KeyT] (piID:KeyT, start:Long=System.currentTimeMillis(), calls:Int=0, finish:Option[Long]=None, result:Option[String]=None) {
  def complete(time:Long,result:Any) = copy(finish=Some(time),result=Some(result.toString))
  def call = copy(calls=calls+1)
}

/** Collects/aggregates metrics across multiple process calls and workflow executions
  *
  */
class MetricsAggregator[KeyT] {
  import scala.collection.immutable.Map

  /** Process metrics indexed by workflow ID, then by call reference ID */
  val processMap = scala.collection.mutable.Map[KeyT,Map[Int,ProcessMetrics[KeyT]]]()
  /** Workflow metrics indexed by workflow ID */
  val workflowMap = scala.collection.mutable.Map[KeyT,WorkflowMetrics[KeyT]]()
  
  // Set
  
  def +=(m:ProcessMetrics[KeyT]) = {
    val old = processMap.get(m.piID) match {
      case None => Map[Int,ProcessMetrics[KeyT]]()
      case Some(map) => map
    }
    processMap += (m.piID -> (old + (m.ref -> m)))
  }
  def +=(m:WorkflowMetrics[KeyT]) = workflowMap += (m.piID->m)
  
  // Update
  
  def ^(piID:KeyT,ref:Int,u:ProcessMetrics[KeyT]=>ProcessMetrics[KeyT]) = 
    processMap.get(piID).flatMap(_.get(ref)).map { m => this += u(m) }
  def ^(piID:KeyT,u:WorkflowMetrics[KeyT]=>WorkflowMetrics[KeyT]) = 
    workflowMap.get(piID).map { m => this += u(m) }
  
  // Handle events
  
  def workflowStart(piID:KeyT, time:Long=System.currentTimeMillis()):Unit =
    this += WorkflowMetrics(piID,time)
  def workflowResult(piID:KeyT, result:Any, time:Long=System.currentTimeMillis()):Unit =
    this ^ (piID,_.complete(time,result))
  def workflowException(piID:KeyT, ex:Throwable, time:Long=System.currentTimeMillis()):Unit =
    this ^ (piID,_.complete(time,"Exception: " + ex.getLocalizedMessage))
  
  def procCall(piID:KeyT, ref:Int, process:String, time:Long=System.currentTimeMillis()):Unit = {
    this += ProcessMetrics(piID,ref,process,time)
    this ^ (piID,_.call)
  }
  def procReturn(piID:KeyT, ref:Int, result:Any, time:Long=System.currentTimeMillis()):Unit =
    this ^ (piID,ref,_.complete(time, result))
  def processException(piID:KeyT, ref:Int, ex:Throwable, time:Long=System.currentTimeMillis()):Unit = processFailure(piID,ref,ex.getLocalizedMessage,time)
  def processFailure(piID:KeyT, ref:Int, ex:String, time:Long=System.currentTimeMillis()):Unit = {
    this ^ (piID,_.complete(time,"Exception: " + ex))
    this ^ (piID,ref,_.complete(time,"Exception: " + ex))
  }
  
  // Getters 
  
  def keys = workflowMap.keys
  def workflowMetrics = workflowMap.values.toSeq.sortBy(_.start)
  def processMetrics = processMap.values.flatMap(_.values).toSeq.sortBy(_.start)
  def processMetricsOf(id:KeyT) = processMap.getOrElse(id,Map[Int,ProcessMetrics[KeyT]]()).values.toSeq.sortBy(_.start)
  def processSet = processMap.values.flatMap(_.values.map(_.process)).toSet[String]
}

class MetricsHandler[KeyT](override val name: String, timeFn: PiMetadata.Key[Long] = PiMetadata.SystemTime )
  extends MetricsAggregator[KeyT] with PiEventHandler[KeyT] {

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

