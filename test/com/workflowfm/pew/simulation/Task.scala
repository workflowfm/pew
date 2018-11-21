package com.workflowfm.pew.simulation

import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TaskTests extends WordSpecLike with Matchers with BeforeAndAfterAll with TaskTester {

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
}


trait TaskTester {
  // create a Task
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
