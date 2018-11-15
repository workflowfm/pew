package com.workflowfm.pew.metrics
import com.workflowfm.pew._

case class ProcessStatistic(name: String, waitTime:Long, busyTime:Long=0){
  def addWaitTime(timedelta:Long) = copy(waitTime=waitTime +timedelta)
  def addBusyTime(timedelta:Long) = copy(busyTime=busyTime +timedelta)
}

sealed trait state{
  def state:String
}

case class Busy(state:String="Busy") extends state{}
case class Idle(state:String="Idle") extends state{}

class WorkflowMetric[KeyT](piID:KeyT, time:Long){

  val startTime:Long = time
  
  // var state:Option[String] = None

  var _wfType:String = "not assigned";
  var totalIdleTime:Long = 0
  var totalBusyTime:Long = 0
  var completionTime:Option[Long] = None
  var processStatistics = scala.collection.mutable.Map[Int,ProcessStatistic]()
  var processCount:Int = 0
  var lastTimeStamp:Long = startTime

  def wfType = _wfType
  
  // later
  // def toggleState():Unit{
  //   state =  state match{
  //     case Busy: Idle()
  //     case Idle: Busy()
  //   }
  // }

  def setLastTimeStamp(time:Long) = {
    if(time > lastTimeStamp) lastTimeStamp=time else throw new Exception("New timestamp older than current timestamp")
  }

  // def updateStatOnCall(old: ProcessStatistic,time: Long):ProcessStatistic = {
  //   old.addWaitTime(time)
  // }

  // def updateStatOnReturn(old: ProcessStatistic,time: Long):ProcessStatistic = {
  //   old.addBusyTime(time)
    
  // }
  
  // some events can be called multiple times like washing or testing
  def handleCallEvent(ref:Int,name:String,time:Long){
    val timedelta = time - lastTimeStamp
    totalIdleTime += timedelta

    val stats = processStatistics.get(ref) match {
      case None => ProcessStatistic(name,timedelta)
      case Some(stats) => stats.addWaitTime(timedelta)
    }
    processStatistics(ref) = stats
    
    setLastTimeStamp(time)
    if(processCount == 0) _wfType = name else processCount += 1
  }

  def handleReturnEvent(ref:Int,time:Long){
    val timedelta = time - lastTimeStamp
    totalBusyTime += timedelta
    val stats = processStatistics.get(ref) match {
      case Some(stats) => stats.addBusyTime(timedelta)
      case None => throw new Exception("Returned process doesn't exist")
    }
    processStatistics(ref) = stats
    setLastTimeStamp(time)
  }

  def handleResultEvent(time:Long){
    completionTime = Some(startTime - time)
  }

}

class WorkflowMapper[KeyT](){
  val workflows = scala.collection.mutable.Map[KeyT,WorkflowMetric[KeyT]]()
  var avgBusyTime = 0
  var avgIdleTime = 0
  var avgDuration = 0
  
  // add new work flow
  def handleStartEvent(piID:KeyT,time:Long){
    val metric = new WorkflowMetric(piID,time)
    workflows += (piID -> metric)
  }

  def handleCallEvent(piID:KeyT,ref:Int,name:String,time:Long){
    val metric = workflows.get(piID) match {
      case None => throw new Error("Process Call to an unregistered workflow")
      case Some(metrics) => metrics
    }
    metric.handleCallEvent(ref,name,time)
  }

  def handleReturnEvent(piID:KeyT,ref:Int,time:Long){
    val metric = workflows.get(piID) match {
      case None => throw new Error("Process Call to an unregistered workflow")
      case Some(metrics) => metrics
    }
    metric.handleReturnEvent(ref,time)
  }

  def handleResultEvent(piID:KeyT,time:Long){
    val metric = workflows.get(piID) match {
      case None => throw new Error("Process Call to an unregistered workflow")
      case Some(metrics) => metrics
    }
    metric.handleResultEvent(time)
  }
  
}

class WorkflowStatsHandler[KeyT](override val name:String) extends WorkflowMapper[KeyT] with PiEventHandler[KeyT] {
  override def apply(e:PiEvent[KeyT]) = { e match {
    case PiEventStart(i,t) => handleStartEvent(i.id,t)
    case PiEventResult(i,r,t) => handleResultEvent(i.id,t)
    // dropping the ref varible since aggregating data per process type
    case PiEventCall(i,r,p,_,t) => handleCallEvent(i,r,p.iname,t)
    case PiEventReturn(i,r,s,t) => handleReturnEvent(i,r,t)
    case PiEventProcessException(i,r,m,_,t) => throw new Exception(m)
    case x:PiExceptionEvent[KeyT] => throw new Exception("Workflow had an exception")
    
  }
  false }
}

