package com.workflowfm.pew.metrics

import scala.collection.immutable.Queue
import com.workflowfm.pew._

case class ProcessMetrics[KeyT] (piID:KeyT, ref:Int, process:String, start:Long=System.currentTimeMillis(), finish:Option[Long]=None, result:Option[String]=None) {
  def complete(time:Long,result:Any) = copy(finish=Some(time),result=Some(result.toString))
}

case class WorkflowMetrics[KeyT] (piID:KeyT, start:Long=System.currentTimeMillis(), calls:Int=0, finish:Option[Long]=None, result:Option[String]=None) {
  def complete(time:Long,result:Any) = copy(finish=Some(time),result=Some(result.toString))
  def call = copy(calls=calls+1)
}

class MetricsAggregator[KeyT] {
  import scala.collection.immutable.Map
  
  val processMap = scala.collection.mutable.Map[KeyT,Map[Int,ProcessMetrics[KeyT]]]()
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

class MetricsHandler[KeyT](override val name:String) extends MetricsAggregator[KeyT] with PiEventHandler[KeyT] {
  override def apply(e:PiEvent[KeyT]) = { e match {
    case PiEventStart(i,t) => workflowStart(i.id,t)
    case PiEventResult(i,r,t) => workflowResult(i.id,r,t)
    case PiEventCall(i,r,p,_,t) => procCall(i,r,p.iname,t)
    case PiEventReturn(i,r,s,t) => procReturn(i,r,s,t)
    case PiEventProcessException(i,r,m,_,t) => processFailure(i,r,m,t)
    case x:PiExceptionEvent[KeyT] => workflowException(x.id,x.exception,x.time)
  }
  false }
}

