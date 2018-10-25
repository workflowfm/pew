package com.workflowfm.pew.metrics

import scala.collection.immutable.Queue
import com.workflowfm.pew._

case class ProcessMetrics[KeyT] (piID:KeyT, ref:Int, process:String, start:Long=System.nanoTime(), finish:Option[Long]=None, result:Option[String]=None) {
  def complete(time:Long,result:Any) = copy(finish=Some(time),result=Some(result.toString))
}

case class WorkflowMetrics[KeyT] (piID:KeyT, start:Long=System.nanoTime(), calls:Int=0, finish:Option[Long]=None, result:Option[String]=None) {
  def complete(time:Long,result:Any) = copy(finish=Some(time),result=Some(result.toString))
  def call = copy(calls=calls+1)
}

class MetricsAggregator[KeyT] {
  import scala.collection.mutable.Map
  val processMetrics = Map[(KeyT,Int),ProcessMetrics[KeyT]]()
  val workflowMetrics = Map[KeyT,WorkflowMetrics[KeyT]]()
  
  def +=(m:ProcessMetrics[KeyT]) = processMetrics += ((m.piID,m.ref)->m)
  def +=(m:WorkflowMetrics[KeyT]) = workflowMetrics += (m.piID->m)
  
  def ^(piID:KeyT,ref:Int,u:ProcessMetrics[KeyT]=>ProcessMetrics[KeyT]) = 
    processMetrics.get((piID,ref)).map { m => this += u(m) }
  def ^(piID:KeyT,u:WorkflowMetrics[KeyT]=>WorkflowMetrics[KeyT]) = 
    workflowMetrics.get(piID).map { m => this += u(m) }
  
  def workflowStart(piID:KeyT, time:Long=System.nanoTime()):Unit =
    this += WorkflowMetrics(piID,time)
  def workflowResult(piID:KeyT, result:Any, time:Long=System.nanoTime()):Unit =
    this ^ (piID,_.complete(time,result))
  def workflowException(piID:KeyT, ex:Throwable, time:Long=System.nanoTime()):Unit =
    this ^ (piID,_.complete(time,"Exception: " + ex.getLocalizedMessage))
  
  def procCall(piID:KeyT, ref:Int, process:String, time:Long=System.nanoTime()):Unit = {
    this += ProcessMetrics(piID,ref,process,time)
    this ^ (piID,_.call)
  }
  def procReturn(piID:KeyT, ref:Int, result:Any, time:Long=System.nanoTime()):Unit =
    this ^ (piID,ref,_.complete(time, result))
  def processException(piID:KeyT, ref:Int, ex:Throwable, time:Long=System.nanoTime()):Unit = processFailure(piID,ref,ex.getLocalizedMessage,time)
  def processFailure(piID:KeyT, ref:Int, ex:String, time:Long=System.nanoTime()):Unit = {
    this ^ (piID,_.complete(time,"Exception: " + ex))
    this ^ (piID,ref,_.complete(time,"Exception: " + ex))
  }
}

class MetricsHandler[KeyT](override val name:String) extends MetricsAggregator[KeyT] with PiEventHandler[KeyT] {
  override def apply(e:PiEvent[KeyT]) = { e match {
    case PiEventStart(i,t) => workflowStart(i.id,t)
    case PiEventResult(i,r,t) => workflowResult(i.id,r,t)
    case PiEventCall(i,r,p,_,t) => procCall(i,r,p.name,t)
    case PiEventReturn(i,r,s,t) => procReturn(i,r,s,t)
    case PiEventProcessException(i,r,m,_,t) => processFailure(i,r,m,t)
    case x:PiExceptionEvent[KeyT] => workflowException(x.id,x.exception,x.time)
  }
  false }
}

