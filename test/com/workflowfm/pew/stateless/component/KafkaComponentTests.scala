package com.workflowfm.pew.stateless.component

import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.stateless.{CallRef, KafkaTests}
import com.workflowfm.pew.{PiInstance, PiObject}
import org.bson.types.ObjectId
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

abstract class KafkaComponentTests
  extends FlatSpec
  with Matchers
  with BeforeAndAfterAll
  with KafkaTests {

  val p1 = PiInstance( ObjectId.get, pbi, PiObject(1) )
  val p2 = PiInstance( ObjectId.get, pbi, PiObject(1) )

  val eg1 = new {

    val r1 = ( CallRef(0), PiObject((1, 3)) )
    val r2 = ( CallRef(1), PiObject("R1") )
    val r3 = ( CallRef(2), PiObject("R2") )

    val pNew: PiInstance[ObjectId]
      = PiInstance( ObjectId.get, ri, PiObject(13) )

    val pInProgress: PiInstance[ObjectId]
      = pNew
        .reduce
        .handleThreads((_, _) => true)._2

    val pFinishing: PiInstance[ObjectId]
      = pInProgress
        .postResult( r1._1.id, r1._2 )
        .reduce
        .handleThreads((_, _) => true)._2

    val pCompleted: PiInstance[ObjectId]
      = pFinishing
        .postResult( r2._1.id, r2._2 )
        .postResult( r3._1.id, r3._2 )
        .reduce

  }

  val ree = RemoteExecutorException("test")

  def update( pii: PiInstance[ObjectId], part: Int ): (PiiHistory, Int)
    = ( PiiUpdate( pii ), part )

  val result = (CallRef(0), PiObject(0))

  def seqreq( pii: PiInstance[ObjectId], res: (CallRef, PiObject), part: Int ): (PiiHistory, Int)
    = ( SequenceRequest( pii.id, res ), part )

  def seqfail( pii: PiInstance[ObjectId], ref: CallRef, part: Int ): (PiiHistory, Int)
    = ( SequenceFailure( pii.id, ref, RemoteExecutorException("test") ), part )
}
