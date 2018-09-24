package com.workflowfm.pew.simulation

import scala.collection.mutable.Queue
import com.workflowfm.pew.metrics._

object TaskResource {
  sealed trait State
  case object Busy extends State
  case class Finished(t:Task) extends State
  case object Idle extends State
}

class TaskResource(val name:String,val costPerTick:Int) extends ResourceMetricTracker { 
  var currentTask :Option[(Int,Int,Task)] = None
  var lastUpdate :Int = 1
  
  def isIdle :Boolean = currentTask == None 
  
  def finishTask(currentTime:Int) :Option[Task] = currentTask match {
    case None => {
        //println("["+currentTime+"] \"" + name + "\" is idle.")
        None
    }
    case Some((startTime,duration,task)) => 
      if (currentTime >= startTime + duration) {
        println("["+currentTime+"] \"" + name + "\" detached from task \"" + task.name + " (" + task.simulation +")\".")
        currentTask = None
        lastUpdate = currentTime
        Some(task)
      }
      else {
        //println("["+currentTime+"] \"" + name + "\" is attached to task \"" + task.name + " (" + task.simulation +")\" - " + (startTime + duration - currentTime) + " ticks remaining.")
        None
      }
  }
  
  def startTask(task:Task,currentTime:Int,duration:Int) = {
    currentTask match {
      case None => {
        println("["+currentTime+"] \"" + name + "\" is NOW attached to task \"" + task.name + " (" + task.simulation +")\" - " + duration + " ticks remaining.")
        currentTask = Some(currentTime,duration,task)
        idle(currentTime-lastUpdate)
        lastUpdate = currentTime
        resStart(currentTime)
        true
      }
      case Some((_,_,currentTask)) => {
        println("["+currentTime+"] <*> <*> <*> ERROR <*> <*> <*> \"" + name + "\" tried to attach to \"" + task.name + " (" + task.simulation +")\" but is already attached to \"" + currentTask.name + "\"!")
        false
      }
    }
  }
  
  
  def nextAvailableTimestamp(currentTime:Int) :Int = currentTask match {
    case None => currentTime
    case Some((startTime,actualDuration,t)) => {
      startTime + t.duration.estimate
    }
  }
    
}