package com.workflowfm.pew.simulation

import scala.collection.mutable.Queue

object TaskResource {
  sealed trait State
  case object Busy extends State
  case class Finished(t:Task) extends State
  case object Idle extends State
}

class TaskResource(val name:String,val costPerTick:Int) extends ResourceMetricTracker { 
  var currentTask :Option[(Int,Int,Task)] = None
  
  def isIdle :Boolean = currentTask == None 
  
  def tick(t:Int) :TaskResource.State = currentTask match {
    case None => {
        println("["+t+"] \"" + name + "\" is idle.")
        TaskResource.Idle
    }
    case Some((startTime,duration,task)) => 
      if (t > startTime + duration) {
        println("["+t+"] \"" + name + "\" detached from task \"" + task.name + " (" + task.simulation +")\".")
        task.execute(t)
        currentTask = None
        TaskResource.Finished(task)
      }
      else {
        println("["+t+"] \"" + name + "\" is attached to task \"" + task.name + " (" + task.simulation +")\" - " + (startTime + duration - t) + " ticks remaining.")
        TaskResource.Busy
      }
  }
  
  def startTask(task:Task,currentTime:Int) = {
    currentTask match {
      case None => {
        val duration = task.duration.get -1
        println("["+currentTime+"] \"" + name + "\" is NOW attached to task \"" + task.name + " (" + task.simulation +")\" - " + duration + " ticks remaining.")
        currentTask = Some(currentTime,duration,task)
        true
      }
      case Some((_,_,currentTask)) => {
        println("["+currentTime+"] <*> <*> <*> ERROR <*> <*> <*> \"" + name + "\" tried to attach to \"" + task.name + " (" + task.simulation +")\" but is already attached to \"" + currentTask.name + "\"!")
        false
      }
    }
  }
  
//  def +=(t:Task) = {
//    println("[Resource] \"" + name + "\" added task \"" + t.name + "\" to the queue.")
//    queue+=t
//  }
  
  def nextAvailableTimestamp(currentTime:Int) :Int = currentTask match {
    case None => currentTime
    case Some((startTime,actualDuration,t)) => {
      startTime + t.duration.estimate
    }
  }
    
}