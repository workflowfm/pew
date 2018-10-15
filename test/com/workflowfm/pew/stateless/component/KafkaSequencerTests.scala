package com.workflowfm.pew.stateless.component

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaWrapperFlows.flowSequencer
import com.workflowfm.pew.stateless.instances.kafka.components.MockTracked
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KafkaSequencerTests extends KafkaComponentTests  {

  def runSequencer(history: (PiiHistory, Int)*): Seq[MockTracked[Seq[AnyMsg]]]
    = await(
        MockTracked
        .source(history)
        .groupBy(Int.MaxValue, _.part)
        .via(flowSequencer)
        .mergeSubstreams
        .runWith(Sink.seq)(ActorMaterializer())
      )

  it should "sequence 1 PiiUpdate and 1 SequenceRequest" in {
    val res = runSequencer(
      update(p1, 1),
      seqreq(p1, result, 1)
    )

    res should have size 1
    res.head.value should have size 1
    res.head.consuming shouldBe 2
  }

  it should "sequence 2 PiiUpdates from different Piis and 1 SequenceRequest" in {
    val res = runSequencer(
      update(p1, 1),
      update(p2, 1),
      seqreq(p1, result, 1)
    )

    res should (have size 1)
    res.head.value should (have size 2)
    res.head.consuming shouldBe 3
  }

  it should "sequence 4 PiiUpdate of 2 Piis and 1 SequenceRequest" in {
    val res = runSequencer(
      update(p1, 1),
      update(p2, 1),
      update(p1, 1),
      update(p2, 1),
      seqreq(p1, result, 1),
    )

    res should (have size 1)
    res.head.value should (have size 2)
    res.head.consuming shouldBe 5
  }

  it should "sequence only 1 of 2 PiiUpdates on different partitions with 1 SequenceRequest" in {
    val res = runSequencer(
      update(p1, 1),
      update(p2, 2),
      seqreq(p1, result,1),
    )

    res should (have size 1)
    res.head.value should (have size 1)
    res.head.consuming shouldBe 2
  }

  it should "not sequence a PiiUpdate and SequenceRequest for different Piis" in {
    runSequencer(
      update(p1, 1),
      seqreq(p2, result, 1),

    ) shouldBe empty
  }

  it should "not sequence a PiiUpdate and SequenceRequest on different partitions" in {
    runSequencer(
      update(p1, 1),
      seqreq(p1, result, 2),

    ) shouldBe empty
  }

  it should "correctly complete a SequenceFailure" in {
    val res = runSequencer(
      update(eg1.pInProgress, 1),
      seqfail(eg1.pInProgress, eg1.r1._1, 1)
    )

    res should (have size 1)
    res.head.consuming shouldBe 2

    val msgsOf = new MessageMap( res.head.value )
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[PiiResult[_]] should have size 1
  }

  it should "correctly complete a tangled SequenceFailure" in {
    val res = runSequencer(
      seqreq(p1, result, 1),
      update(eg1.pFinishing, 1),
      seqfail(eg1.pFinishing, eg1.r2._1, 1),
      seqfail(eg1.pFinishing, eg1.r3._1, 1),
      update(p1, 1),
    )

    res should (have size 1)
    res.head.consuming shouldBe 5

    val msgsOf = new MessageMap( res.head.value )
    msgsOf[ReduceRequest] should have size 1
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[PiiResult[_]] should have size 1
  }

  it should "correctly send a partially complete SequenceFailure" in {
    val res = runSequencer(
      seqreq(p1, result, 1),
      update(eg1.pFinishing, 1 ),
      seqfail(eg1.pFinishing, eg1.r2._1, 1),
      update(p1, 1),
    )

    res should (have size 1)
    res.head.consuming shouldBe 4

    val msgsOf = new MessageMap( res.head.value )
    msgsOf[ReduceRequest] should have size 1
    msgsOf[SequenceFailure] should have size 1
    msgsOf[PiiResult[_]] shouldBe empty
  }

  it should "not send an incomplete SequenceFailure" in {
    val res = runSequencer(
      seqreq(p1, result, 1),
      seqfail(eg1.pFinishing, eg1.r2._1, 1),
      update(p1, 1),
    )

    res shouldBe empty
  }

  it should "correctly resume and complete a partial SequenceFailure" in {
    val res = runSequencer(
      ( SequenceFailure( eg1.pFinishing, Seq(), Seq((eg1.r2._1, ree)) ), 1 ),
      seqreq( eg1.pFinishing, eg1.r3, 1 )
    )

    res should (have size 1)
    res.head.consuming shouldBe 2

    val msgsOf = new MessageMap( res.head.value )
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[PiiResult[_]] should have size 1
  }

  it should "correctly handle an outstanding irreducible PiiUpdate" in {
    val res = runSequencer(
      update( eg1.pInProgress, 1 ),
      update( p1, 1 ),
      seqreq( p1, result, 1 ),
    )

    res should (have size 1)
    res.head.consuming shouldBe 3

    val msgsOf = new MessageMap( res.head.value )
    msgsOf[ReduceRequest] should have size 2
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[PiiResult[_]] shouldBe empty
  }

}