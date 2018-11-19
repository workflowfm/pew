package com.workflowfm.pew.simulation

import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SchedulerTests extends WordSpecLike with Matchers with BeforeAndAfterAll
    with TaskTester with ScheduleTester {

  "The Schedule" must {

    "fit a task at the edge of another" in {
      s((1,2)) + (2,3)  should be (Some(s((1,3))))
      s((3,4)) + (2,3)  should be (Some(s((2,4))))
    }

    "fit a task at the edge of two others" in {
      s((1,2),(3,4)) + (2,3)  should be (Some(s((1,4))))
      s((1,2),(4,5)) + (2,3)  should be (Some(s((1,3),(4,5))))
      s((1,2),(4,5)) + (3,4)  should be (Some(s((1,2),(3,5))))
    }

    "fit a task in gaps" in {
      s((1,2)) + (3,4) should be (Some(s((1,2),(3,4))))
      s((3,4)) + (1,2) should be (Some(s((1,2),(3,4))))
      s((1,2),(3,4)) + (5,6) should be (Some(s((1,2),(3,4),(5,6))))
      s((1,2),(5,6)) + (3,4) should be (Some(s((1,2),(3,4),(5,6))))
      s((3,4),(5,6)) + (1,2) should be (Some(s((1,2),(3,4),(5,6))))
    }

    "not fit tasks that clash with the start of another task" in {
      s((1,2)) + (1,3) should be (None)
      s((1,4)) + (1,3) should be (None)
      s((2,3)) + (1,3) should be (None)
      s((2,4)) + (1,3) should be (None)
    }

    "not fit tasks that clash with the end of another task" in {
      s((2,4)) + (3,4) should be (None)
      s((3,4)) + (2,4) should be (None)
      s((1,4)) + (1,5) should be (None)
      s((1,5)) + (1,4) should be (None)
      s((2,3)) + (1,3) should be (None)     
    }

    "not fit tasks that overlap with another task" in {
      s((1,3)) + (1,3) should be (None)
      s((1,4)) + (2,3) should be (None)
      s((2,3)) + (1,4) should be (None)
    }

    "not fit tasks that clash with two other tasks" in {
      s((1,2),(4,6)) + (2,5) should be (None)
      s((1,2),(4,6)) + (3,5) should be (None)
      s((1,2),(4,6)) + (2,6) should be (None)
      s((1,2),(4,6)) + (3,6) should be (None)
      s((1,2),(4,6)) + (2,7) should be (None)
      s((1,2),(4,6)) + (3,7) should be (None)
      s((1,3),(4,6)) + (2,4) should be (None)
    }

    "merge single tasks with common start" in {
      s((1,2)) ++ s((1,2)) should be (s((1,2)))
      s((1,2)) ++ s((1,3)) should be (s((1,3)))
      s((1,3)) ++ s((1,2)) should be (s((1,3)))
    }

    "merge single tasks with common finish" in {
      s((1,3)) ++ s((2,3)) should be (s((1,3)))
      s((2,3)) ++ s((1,3)) should be (s((1,3)))
    }

    "merge single tasks that don't overlap" in {
      s((1,2)) ++ s((2,3)) should be (s((1,3)))
      s((1,2)) ++ s((3,4)) should be (s((1,2),(3,4)))
      s((1,2),(5,6)) ++ s((3,4)) should be (s((1,2),(3,4),(5,6)))
      s((2,3)) ++ s((1,2)) should be (s((1,3)))
      s((3,4)) ++ s((1,2)) should be (s((1,2),(3,4)))
      s((3,4)) ++ s((1,2),(5,6)) should be (s((1,2),(3,4),(5,6)))
    }

    "merge multiple tasks with one overlapping task" in {
      s((1,2),(3,4),(5,6)) ++ s((1,6)) should be (s((1,6)))
      s((1,2),(3,4),(5,6)) ++ s((0,6)) should be (s((0,6)))
      s((1,2),(3,4),(5,6)) ++ s((1,7)) should be (s((1,7)))
      s((1,2),(3,4),(5,6)) ++ s((0,7)) should be (s((0,7)))
      s((1,6)) ++ s((1,2),(3,4),(5,6)) should be (s((1,6)))
      s((0,6)) ++ s((1,2),(3,4),(5,6)) should be (s((0,6)))
      s((1,7)) ++ s((1,2),(3,4),(5,6)) should be (s((1,7)))
      s((0,7)) ++ s((1,2),(3,4),(5,6)) should be (s((0,7)))
    }

    "merge multiple overlapping tasks" in {
      s((1,2),(3,4),(5,6)) ++ s((2,3),(4,5),(6,7)) should be (s((1,7)))
      s((1,2),(3,4),(5,6)) ++ s((2,3),(4,5)) should be (s((1,6)))
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
        t(2L,Seq("A"),Task.VeryLow,0L,2L)) should be (None)
    }

    "select a lower priority task if it will finish on time" in {
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

    "consider all higher priority tasks for availability" in {
      val m = new TestResourceMap("A","B") + ("B",1L)
      m.s("A",
        t(1L,Seq("B"),Task.Highest),
        t(2L,Seq("A","B"),Task.Medium),
        t(3L,Seq("A"),Task.VeryLow,0L,2L)) should be (Some(3L))
    }

  }

}

trait ScheduleTester {
  def s(l:(Long,Long)*) = Schedule(l.toList)
}

class TestResourceMap(names:String*) {
  // create a resource map
  val m:Map[String,TaskResource] = Map[String,TaskResource]() ++ (names map { n => (n,r(n)) })

  // create a resource
  def r(name:String) = new TaskResource(name,0)

  // pre-attach Tasks to resources
  def +(r:String,duration:Long):TestResourceMap = {
    m.get(r).map { _.startTask(new Task(0,"_","_",0L,Seq(r),duration,duration,0L), 0L) }
    this
  }

  // test DefaultScheduler
  def s(resource:String, tasks:Task*):Option[Long] =
    DefaultScheduler.getNextTask(resource, 0L, m, tasks) map (_.id)

}
