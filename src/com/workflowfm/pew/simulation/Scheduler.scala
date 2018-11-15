package com.workflowfm.pew.simulation

trait Scheduler {
  def getNextTask(resource:String, ticks:Long, resourceMap:Map[String,TaskResource], tasks:Seq[Task]) :Option[Task]
    
  def isIdleResource(r:String, resourceMap:Map[String,TaskResource]) = resourceMap.get(r) match {
      case None => false
      case Some(s) => s.isIdle
  }  
  
}

object DefaultScheduler extends Scheduler {
  def nextEstimatedTaskStart(t:Task, ticks:Long, resourceMap:Map[String,TaskResource], tasks:Seq[Task]) = {
    val precedingTasks = tasks filter (_ < t)
    t.nextPossibleStart(ticks, resourceMap) + (0L /: precedingTasks)(_ + _.estimatedDuration)
  }
  
  def getNextTask(resource:String, ticks:Long, resourceMap:Map[String,TaskResource], tasks:Seq[Task]) :Option[Task] = {
    val relevant = tasks.toList filter (_.resources.contains(resource)) sorted
    val pairs = relevant map {t => (t,nextEstimatedTaskStart(t,ticks,resourceMap,relevant))}
    val canStart = pairs filter { case (t,s) => 
      // s == ticks && // s may be overestimating the start time. having all the resources idle should be enough
      t.resources.forall(isIdleResource(_,resourceMap)) && // all resources are idle
      // all other relevant tasks are either 
      // (1) the same task (name) or 
      // (2) lower or equal priority or 
      // (3) their estimated start time is later than our estimated finish time
      pairs.forall({case (t2,s2) => t.id == t2.id || t <= t2 || ticks + t.estimatedDuration <= s2})}
    if (canStart.isEmpty) None else Some(canStart.head._1)
  }
}
