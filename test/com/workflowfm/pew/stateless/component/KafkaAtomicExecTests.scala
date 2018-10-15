package com.workflowfm.pew.stateless.component

import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.StatelessMessages.{Assignment, SequenceRequest}
import com.workflowfm.pew.stateless.components.AtomicExecutor
import com.workflowfm.pew.{PiInstance, PiItem, PiObject}
import org.bson.types.ObjectId
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KafkaAtomicExecTests extends KafkaComponentTests {

  it should "respond to Assignments with a correct SequenceRequest" in {
    val atomExec: AtomicExecutor = AtomicExecutor()

    val (threads, pii)
    = PiInstance( ObjectId.get, pbi, PiObject(1) )
      .reduce.handleThreads( (_, _) => true )

    val t: Int = threads.head

    val task =
      Assignment(
        pii, CallRef(t) , "Pb",
        pii.piFutureOf(t).get.args
      )

    val response = SequenceRequest( pii.id, ( CallRef(t), PiItem("PbISleptFor1s") ) )

    await( atomExec.respond( task ) ) shouldBe response
  }

}
