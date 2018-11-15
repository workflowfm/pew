package com.workflowfm.pew.simulation

import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SchedulerTests extends WordSpecLike with Matchers with BeforeAndAfterAll with SchedulerTester {

  "Task priority" must {

    "prioritize higher priority" in {
      t(2L,Seq("A"),Task.High,2L,1L,0) < t(1L,Seq("A","B"),Task.Medium,1L,2L,1) should be (true)
    }

    "prioritize old age" in {
      t(2L,Seq("A","B"),Task.Medium,2L,1L,0) > t(1L,Seq("A"),Task.Medium,1L,2L,1) should be (true)
    }

    "prioritize more resources" in {
      t(2L,Seq("A","B"),Task.Medium,0L,1L,0) < t(1L,Seq("A"),Task.Medium,0L,2L,1) should be (true)
    }

    "prioritize longer duration" in {
      t(2L,Seq("A"),Task.Medium,0L,1L,0) > t(1L,Seq("A"),Task.Medium,0L,2L,1) should be (true)
    }

    "prioritize lower interrupt" in {
      t(2L,Seq("A"),Task.Medium,0L,1L,0) < t(1L,Seq("A"),Task.Medium,0L,1L,1) should be (true)
    }

    "prioritize lower ID if all else fails" in {
      t(2L,Seq("A"),Task.Medium,0L,1L,0) > t(1L,Seq("A"),Task.Medium,0L,1L,0) should be (true)
    }

  }

  "The DefaultScheduler" must {

    "select a single task" in {
      val m = new TestResourceMap("A")
      m.s("A",t(1L,Seq("A"))) should be (Some(1L))
    }

    "select an earlier task" in {
      val m = new TestResourceMap("A")
      m.s("A",
        t(1L,Seq("A"),Task.Medium,2L),
        t(2L,Seq("A"),Task.Medium,1L)) should be (Some(2L))
    }

    "not select a blocked task" in {
      val m = new TestResourceMap("A","B") + ("B",1L)
      m.s("A",
        t(1L,Seq("A","B"),Task.Highest),
        t(2L,Seq("A"),Task.VeryLow)) should be (Some(2L))
    }

    "not block higher priority tasks" in {
      val m = new TestResourceMap("A","B") + ("B",1L)
      //DefaultScheduler.nextEstimatedTaskStart(t(1L,Seq("A","B"),Task.Highest), 0L, m.m,Seq( t(1L,Seq("A","B"),Task.Highest),t(2L,Seq("A"),Task.VeryLow,0L,100L))) should be (1L)
      m.s("A",
        t(1L,Seq("A","B"),Task.Highest),
        t(2L,Seq("A"),Task.VeryLow,0L,100L)) should be (None)
    }

    "not block higher priority tasks based on ordering" in {
      val m = new TestResourceMap("A","B") + ("B",1L)
      m.s("A",
        t(1L,Seq("A","B"),Task.Medium,0L),
        t(2L,Seq("A"),Task.Medium,2L,100L)) should be (None)
    }

    "not block higher priority tasks of other resources" in {
      val m = new TestResourceMap("A","B") //+ ("B",1L)
      m.s("A",
        t(1L,Seq("B"),Task.Highest),
        t(2L,Seq("A","B"),Task.VeryLow,0L,100L)) should be (None)
    }

  }

}


trait SchedulerTester {
  def t(
    id:Long,
    resources:Seq[String],
    priority:Task.Priority=Task.Medium,
    created:Long = 0L,
    duration:Long = 1L,
    interrupt:Int = 0,
    name:String="X"
  ) =
    new Task(id,name,"Test",created,resources,duration,duration,0L,interrupt,priority)
}

class TestResourceMap(names:String*) {
  val m:Map[String,TaskResource] = Map[String,TaskResource]() ++ (names map { n => (n,r(n)) })

  def r(name:String) = new TaskResource(name,0)

  def +(r:String,duration:Long):TestResourceMap = {
    m.get(r).map { _.startTask(new Task(0,"_","_",0L,Seq(r),duration,duration,0L), 0L) }
    this
  }

  def s(resource:String, tasks:Task*):Option[Long] =
    DefaultScheduler.getNextTask(resource, 0L, m, tasks) map (_.id)

}
