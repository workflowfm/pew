package com.workflowfm.pew.simulation

import scala.annotation.tailrec

trait Scheduler {
  def getNextTasks(tasks:Seq[Task], currentTime:Long, resourceMap:Map[String,TaskResource]) :Seq[Task]

  def isIdleResource(r:String, resourceMap:Map[String,TaskResource]) = resourceMap.get(r) match {
      case None => false
      case Some(s) => s.isIdle
  }  
  
}

object DefaultScheduler extends Scheduler {
  import scala.collection.immutable.Queue

  override def getNextTasks(tasks:Seq[Task], currentTime:Long, resourceMap:Map[String,TaskResource]) :Seq[Task] =
    findNextTasks(currentTime, resourceMap, resourceMap.mapValues(Schedule(_)), tasks.toList, Queue())

  @tailrec
  def findNextTasks(
    currentTime:Long,
    resourceMap:Map[String,TaskResource],
    schedules:Map[String,Schedule],
    tasks:List[Task],
    result:Queue[Task]
  ):Seq[Task] = tasks match {
    case Nil => result
    case t :: rest => {
      val start = Schedule.merge(t.resources.flatMap(schedules.get(_))) ? (currentTime,t)
      val schedules2 = (schedules /: t.resources) {
        case (s,r) => s + (r -> (s.getOrElse(r,Schedule(Nil)) +> (start,t)))
      }
      val result2 = if (start == currentTime && t.taskResources(resourceMap).forall(_.isIdle)) result :+ t else result
      findNextTasks(currentTime, resourceMap, schedules2, rest, result2)
    }
  }    
}

case class Schedule(tasks:List[(Long,Long)]) {

  def +(start:Long,end:Long):Option[Schedule] = Schedule.add(start,end,tasks) match {
    case None => None
    case Some(l) => Some(copy(tasks=l))
  }

  def +>(start:Long,end:Long):Schedule = Schedule.add(start,end,tasks) match {
    case None => {
      System.err.println(s"*** Unable to add ($start,$end) to Schedule: $tasks")
      this
    }
    case Some(l) => copy(tasks=l)
  }

  def +>(startTime:Long, t:Task):Schedule = this +> (startTime,startTime+t.estimatedDuration)

  def ?(currentTime:Long, t:Task):Long = Schedule.fit(currentTime,t.estimatedDuration,tasks)


  def ++(s:Schedule):Schedule = Schedule(Schedule.merge(tasks,s.tasks))

  def isValid = Schedule.isValid(tasks)
}

object Schedule {
  import scala.collection.immutable.Queue

  def apply(r:TaskResource):Schedule = r.currentTask match {
    case None => Schedule(List())
    case Some((s,t)) => Schedule((s,s + t.estimatedDuration) :: Nil)
  }

  @tailrec
  def add (
    start:Long, end:Long,
    tasks:List[(Long,Long)],
    result:Queue[(Long,Long)] = Queue[(Long,Long)]()
  ):Option[List[(Long,Long)]] = tasks match {
    case Nil => Some(result :+ (start,end) toList)
    case (l:Long,r:Long) :: t =>
      if (l > end) Some(result ++ ((start,end) :: (l,r) :: t) toList)
      else if (l == end) Some(result ++ ((start,r) :: t) toList)
      else if (r < start) add(start,end,t,result :+ ((l,r)))
      else if (r == start) add(l,end,t,result)
      else /* if (r >= end) */ None
      //else None
  }

  @tailrec
  def fit (
    start:Long,
    duration:Long,
    tasks:List[(Long,Long)]
  ):Long = tasks match {
    case Nil => start
    case (l,_) :: _ if (l >= start + duration) => start
    case (_,r) :: t => fit(r,duration,t)
  }

  @tailrec
  def merge(
    g1:List[(Long,Long)],
    g2:List[(Long,Long)],
    result:Queue[(Long,Long)] = Queue[(Long,Long)]()
  ):List[(Long,Long)] = g1 match {
    case Nil => result ++ g2 toList
    case (l1,r1) :: t1 => g2 match {
      case Nil => result ++ g1 toList
      case (l2,r2) :: t2 => {
        if (r2 < l1) merge(g1,t2,result :+ (l2,r2))
        else if (r1 < l2) merge(t1,g2,result :+ (l1,r1))
        else if (r1 == r2) merge(t1,t2,result :+ (math.min(l1,l2),r1))
        else if (r2 == l1) merge((l2,r1)::t1,t2,result)
        else if (r1 == l2) merge(t1,(l1,r2)::t2,result)
        else if (r1 < r2) merge(t1,(math.min(l1,l2),r2)::t2,result)
        else /* if (r1 > r2)*/ merge((math.min(l1,l2),r1)::t1,t2,result)
      }
    }
  }

  def merge(schedules:Seq[Schedule]):Schedule = {
    (Schedule(List()) /: schedules)(_ ++ _)
  }

  @deprecated("No longer using gaps in Schedule","1.2.0")
  def fitInGaps (
    start:Long, end:Long,
    gaps:List[(Long,Long)],
    result:Queue[(Long,Long)] = Queue[(Long,Long)]()
  ):Option[List[(Long,Long)]] = gaps match {
    case Nil => Some(result.toList)
    case (l:Long,r:Long) :: t =>
      if (l == start && end == r) fitInGaps(start,end,t,result) // event fits exactly
      else if (l == start && end <= r) fitInGaps(start,end,t,result :+ ((end,r)) )// add an event at the beginning of the gap
      else if (l <= start && end == r) fitInGaps(start,end,t,result :+ ((l,start)) ) // add an event at the end of the gaps
      else if (l < start && end < r) fitInGaps(start,end,t,result :+ ((l,start)) :+ ((end,r)) ) // add an event within a gap
      else if (start > r || end < l) fitInGaps(start,end,t,result :+ ((l,r)) )
      else None
  }

  @deprecated("No longer using gaps in Schedule","1.2.0")
  // we assume all gap lists finish with a (t,Long.MaxValue) gap
  def mergeGaps (
    g1:List[(Long,Long)],
    g2:List[(Long,Long)],
    result:Queue[(Long,Long)] = Queue[(Long,Long)]()
  ):List[(Long,Long)] = g1 match {
    case Nil => result toList
    case (l1,r1) :: t1 => g2 match {
      case Nil => result toList
      case (l2,r2) :: t2 => {
        if (r2 <= l1) mergeGaps(g1,t2,result)
        else if (r1 <= l2) mergeGaps (t1,g2,result)
        else if (r1 == Long.MaxValue && r1 == r2) result :+ (math.max(l1,l2),r1) toList
        else if (r2 <= r1) mergeGaps(g1,t2,result :+ (math.max(l1,l2),r2))
        else /* if (r1 < r2) */ mergeGaps(t1,g2,result :+ (math.max(l1,l2),r1))
      }
    }
  }

  def isValid(gaps:List[(Long,Long)], end:Long = Long.MinValue):Boolean = gaps match {
    case Nil => true
    case (l,r) :: t if end < l && l < r => isValid(t, r)
    case _ => false
  }
}
