package com.workflowfm.pew.simulation

import scala.collection.mutable.Queue
import com.workflowfm.pew.simulation.metrics._

object TaskResource {
  sealed trait State
  case object Busy extends State
  case class Finished(t:Task) extends State
  case object Idle extends State
}

class TaskResource(val name:String,val costPerTick:Int) { 
  var currentTask :Option[(Long,Task)] = None
  var lastUpdate :Long = 1
  
  def isIdle :Boolean = currentTask == None 
  
  def finishTask(currentTime:Long) :Option[Task] = currentTask match {
    case None => {
        //println("["+currentTime+"] \"" + name + "\" is idle.")
        None
    }
    case Some((startTime,task)) => 
      if (currentTime >= startTime + task.duration) {
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
  
  def startTask(task:Task,currentTime:Long) = {
    currentTask match {
      case None => {
        println("["+currentTime+"] \"" + name + "\" is NOW attached to task \"" + task.name + " (" + task.simulation +")\" - " + task.duration + " ticks remaining.")
        currentTask = Some(currentTime,task)
        //TODO idle(currentTime-lastUpdate)
        lastUpdate = currentTime
        //TODO resStart(currentTime)
        true
      }
      case Some((_,currentTask)) => {
        println("["+currentTime+"] <*> <*> <*> ERROR <*> <*> <*> \"" + name + "\" tried to attach to \"" + task.name + " (" + task.simulation +")\" but is already attached to \"" + currentTask.name + "\"!")
        false
      }
    }
  }
  
  
  def nextAvailableTimestamp(currentTime:Long) :Long = currentTask match {
    case None => currentTime
    case Some((startTime,t)) => {
      startTime + t.estimatedDuration
    }
  }
  
  
  def update(time:Long) = lastUpdate = time
}