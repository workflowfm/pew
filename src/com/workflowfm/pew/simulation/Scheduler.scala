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

case class Schedule(gaps:List[(Long,Long)]) {
  def +(start:Long,end:Long):Option[Schedule] = Schedule.fit(start,end,gaps) match {
    case None => None
    case Some(l) => Some(copy(gaps=l))
  }

  def +(startTime:Long, t:Task):Option[Schedule] = this + (startTime,startTime+t.estimatedDuration)

  def ++(s:Schedule):Schedule = Schedule(Schedule.merge(gaps,s.gaps))

  def isValid = Schedule.isValid(gaps)
}

object Schedule {
  import scala.collection.immutable.Queue

  def apply(r:TaskResource,currentTime:Long) = r.currentTask match {
    case None => (currentTime,Long.MaxValue)
    case Some((s,t)) => (s + t.estimatedDuration,Long.MaxValue)
  }

  def fit (
    start:Long, end:Long,
    gaps:List[(Long,Long)],
    result:Queue[(Long,Long)] = Queue[(Long,Long)]()
  ):Option[List[(Long,Long)]] = gaps match {
    case Nil => Some(result.toList)
    case (l:Long,r:Long) :: t =>
      if (l == start && end == r) fit(start,end,t,result) // event fits exactly
      else if (l == start && end <= r) fit(start,end,t,result :+ ((end,r)) )// add an event at the beginning of the gap
      else if (l <= start && end == r) fit(start,end,t,result :+ ((l,start)) ) // add an event at the end of the gaps
      else if (l < start && end < r) fit(start,end,t,result :+ ((l,start)) :+ ((end,r)) ) // add an event within a gap
      else if (start > r || end < l) fit(start,end,t,result :+ ((l,r)) )
      else None
  }

  // we assume all gap lists finish with a (t,Long.MaxValue) gap
  def merge (
    g1:List[(Long,Long)],
    g2:List[(Long,Long)],
    result:Queue[(Long,Long)] = Queue[(Long,Long)]()
  ):List[(Long,Long)] = g1 match {
    case Nil => result toList
    case (l1,r1) :: t1 => g2 match {
      case Nil => result toList
      case (l2,r2) :: t2 => {
        if (r2 <= l1) merge(g1,t2,result)
        else if (r1 <= l2) merge (t1,g2,result)
        else if (r1 == Long.MaxValue && r1 == r2) result :+ (math.max(l1,l2),r1) toList
        else if (r2 <= r1) merge(g1,t2,result :+ (math.max(l1,l2),r2))
        else /* if (r1 < r2) */ merge(t1,g2,result :+ (math.max(l1,l2),r1))
      }
    }
  }

  def isValid(gaps:List[(Long,Long)], start:Long = Long.MinValue):Boolean = gaps match {
    case Nil => true
    case (l,r) :: t if start < l && l < r => isValid(t, r)
    case _ => false
  }
}
