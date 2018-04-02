package com.workflowfm.pew.simulation

trait Scheduler {
  def getNextTask(resource:String, ticks:Int, resourceMap:Map[String,TaskResource], tasks:Seq[Task]) :Option[Task]
    
  def isIdleResource(r:String, resourceMap:Map[String,TaskResource]) = resourceMap.get(r) match {
      case None => false
      case Some(s) => s.isIdle
  }  
  
}

object DefaultScheduler extends Scheduler {
  def nextEstimatedTaskStart(t:Task, ticks:Int, resourceMap:Map[String,TaskResource], tasks:Seq[Task]) = {
    val precedingTasks = tasks filter (_ < t)
    t.nextPossibleStart(ticks, resourceMap) + (0 /: precedingTasks)(_ + _.duration.estimate)
  }
  
  def getNextTask(resource:String, ticks:Int, resourceMap:Map[String,TaskResource], tasks:Seq[Task]) :Option[Task] = {
    val relevant = tasks.toList filter (_.resources.contains(resource)) sorted
    val pairs = relevant map {t => (t,nextEstimatedTaskStart(t,ticks,resourceMap,relevant))}
    val canStart = pairs filter { case (t,s) => 
      // s == ticks && // s may be overestimating the start time. having all the resources idle should be enough
      t.resources.forall(isIdleResource(_,resourceMap)) && 
      pairs.forall({case (t2,s2) => t.name == t2.name || t.priority >= t2.priority || ticks + t.duration.estimate < s2})}
    if (canStart.isEmpty) None else Some(canStart.head._1)
  }
}