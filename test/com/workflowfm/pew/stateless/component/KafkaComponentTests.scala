package com.workflowfm.pew.stateless.component

import com.workflowfm.pew.{PiInstance, PiObject}
import com.workflowfm.pew.stateless.{CallRef, KafkaTests}
import com.workflowfm.pew.stateless.StatelessMessages.{PiiHistory, PiiUpdate, ReduceRequest, SequenceRequest}
import org.bson.types.ObjectId
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

abstract class KafkaComponentTests
  extends FlatSpec
  with MockFactory
  with Matchers
  with BeforeAndAfterAll
  with KafkaTests {

  val p1 = PiInstance( ObjectId.get, pbi, PiObject(1) )
  val p2 = PiInstance( ObjectId.get, pbi, PiObject(1) )

  def update( pii: PiInstance[ObjectId], part: Int ): (PiiHistory, Int)
    = ( PiiUpdate( pii ), part )

  val result = (CallRef(0), PiObject(0))

  def seqreq( pii: PiInstance[ObjectId], part: Int ): (PiiHistory, Int)
    = ( SequenceRequest( pii.id, result ), part )
}
