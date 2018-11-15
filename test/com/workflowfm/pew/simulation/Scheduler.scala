package com.workflowfm.pew.simulation

import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SchedulerTests extends WordSpecLike with Matchers with BeforeAndAfterAll
    with TaskTester with ScheduleTester {

  "The Schedule" must {

    "fit a task exactly" in {
      s((1,3)) + (1,3) should be (Some(s()))
      s((1,3),(4,5)) + (1,3) should be (Some(s((4,5))))
      s((1,3),(4,5)) + (4,5) should be (Some(s((1,3))))
    }

    "fit a task at the start" in {
      s((1,3)) + (1,2) should be (Some(s((2,3))))
      s((1,3),(4,5)) + (1,2) should be (Some(s((2,3),(4,5))))
      s((1,3),(4,6)) + (4,5) should be (Some(s((1,3),(5,6))))
    }

    "fit a task at the end" in {
      s((1,3)) + (2,3) should be (Some(s((1,2))))
      s((1,3),(4,5)) + (2,3) should be (Some(s((1,2),(4,5))))
      s((1,3),(4,6)) + (5,6) should be (Some(s((1,3),(4,5))))
    }

    "fit a task in-between" in {
      s((1,4)) + (2,3) should be (Some(s((1,2),(3,4))))
      s((1,4),(5,6)) + (2,3) should be (Some(s((1,2),(3,4),(5,6))))
      s((1,4),(5,8)) + (6,7) should be (Some(s((1,4),(5,6),(7,8))))
    }

    "not fit tasks that don't fit" in {
      s((1,2)) + (1,3) should be (None)
      s((1,2),(3,4)) + (1,3) should be (None)
      s((1,2),(3,4)) + (3,5) should be (None)
      s((2,3)) + (1,3) should be (None)
      s((2,3),(4,5)) + (1,3) should be (None)
      s((2,3),(4,5)) + (3,5) should be (None)
      s((2,3)) + (1,4) should be (None)
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
