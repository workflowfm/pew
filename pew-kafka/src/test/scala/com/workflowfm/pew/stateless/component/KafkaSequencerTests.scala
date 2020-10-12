package com.workflowfm.pew.stateless.component

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import com.workflowfm.pew.PewTestSuite
import com.workflowfm.pew.stateless.KafkaExampleTypes
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.stateless.instances.kafka.components.{ MockTransaction, Tracked }
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaWrapperFlows.flowSequencer

@RunWith(classOf[JUnitRunner])
class KafkaSequencerTests extends PewTestSuite with KafkaExampleTypes {

  def runSequencer(history: (PiiHistory, Int)*): Seq[MockTransaction[MessageMap]] = await(
    MockTransaction
      .source(history)
      .groupBy(Int.MaxValue, _.part)
      .via(flowSequencer)
      .mergeSubstreams
      .runWith(Sink.seq)(ActorMaterializer())
      .map(_.map(Tracked.fmap(new MessageMap(_))))
  )

  it should "sequence 1 PiiUpdate and 1 SequenceRequest" in {
    val res = runSequencer(
      update(p1, 1),
      seqreq(p1, result, 1)
    )

    res should have size 1
    res.head.consuming shouldBe 2
    res.head.value[ReduceRequest] should have size 1
    res.head.value[PiiLog] should have size 1
  }

  it should "sequence 2 PiiUpdates from different Piis and 1 SequenceRequest" in {
    val res = runSequencer(
      update(p1, 1),
      update(p2, 1),
      seqreq(p1, result, 1)
    )

    res should (have size 1)
    res.head.consuming shouldBe 3
    res.head.value[ReduceRequest] should have size 2
    res.head.value[PiiLog] should have size 1
  }

  it should "sequence 4 PiiUpdate of 2 Piis and 1 SequenceRequest" in {
    val res = runSequencer(
      update(p1, 1),
      update(p2, 1),
      update(p1, 1),
      update(p2, 1),
      seqreq(p1, result, 1)
    )

    res should (have size 1)
    res.head.consuming shouldBe 5
    res.head.value[ReduceRequest] should have size 2
    res.head.value[PiiLog] should have size 1
  }

  it should "sequence only 1 of 2 PiiUpdates on different partitions with 1 SequenceRequest" in {
    val res = runSequencer(
      update(p1, 1),
      update(p2, 2),
      seqreq(p1, result, 1)
    )

    res should (have size 1)
    res.head.consuming shouldBe 2

    res.head.value[ReduceRequest] should have size 1
    res.head.value[PiiLog] should have size 1
  }

  it should "not sequence a PiiUpdate and SequenceRequest for different Piis" in {
    runSequencer(
      update(p1, 1),
      seqreq(p2, result, 1)
    ) shouldBe empty
  }

  it should "not sequence a PiiUpdate and SequenceRequest on different partitions" in {
    runSequencer(
      update(p1, 1),
      seqreq(p1, result, 2)
    ) shouldBe empty
  }

  it should "correctly complete a SequenceFailure" in {
    val res = runSequencer(
      update(eg1.pInProgress, 1),
      seqfail(eg1.pInProgress, eg1.r1._1, 1)
    )

    res should (have size 1)
    res.head.consuming shouldBe 2

    res.head.value[ReduceRequest] shouldBe empty
    res.head.value[SequenceFailure] shouldBe empty
    res.head.value[PiiLog] should have size 1

  }

  it should "correctly complete a tangled SequenceFailure" in {
    val res = runSequencer(
      seqreq(p1, result, 1),
      update(eg1.pFinishing, 1),
      seqfail(eg1.pFinishing, eg1.r2._1, 1),
      seqfail(eg1.pFinishing, eg1.r3._1, 1),
      update(p1, 1)
    )

    res should (have size 1)
    res.head.consuming shouldBe 5

    res.head.value[ReduceRequest] should have size 1
    res.head.value[SequenceFailure] shouldBe empty
    res.head.value[PiiLog] should have size 3
  }

  it should "correctly send a partially complete SequenceFailure" in {
    val res = runSequencer(
      seqreq(p1, result, 1),
      update(eg1.pFinishing, 1),
      seqfail(eg1.pFinishing, eg1.r2._1, 1),
      update(p1, 1)
    )

    res should (have size 1)
    res.head.consuming shouldBe 4

    res.head.value[ReduceRequest] should have size 1
    res.head.value[SequenceFailure] should have size 1
    res.head.value[PiiLog] should have size 1
  }

  it should "not send an incomplete SequenceFailure" in {
    runSequencer(
      seqreq(p1, result, 1),
      seqfail(eg1.pFinishing, eg1.r2._1, 1),
      update(p1, 1)
    ) shouldBe empty
  }

  it should "correctly resume and complete a partial SequenceFailure" in {
    val res = runSequencer(
      (eg1.sfFinishing22, 1),
      (eg1.srFinishing3, 1)
    )

    res should (have size 1)
    res.head.consuming shouldBe 2

    res.head.value[ReduceRequest] shouldBe empty
    res.head.value[SequenceFailure] shouldBe empty
    res.head.value[PiiLog] should have size 1
  }

  it should "correctly handle an outstanding irreducible PiiUpdate" in {
    val res = runSequencer(
      update(eg1.pInProgress, 1),
      update(p1, 1),
      seqreq(p1, result, 1)
    )

    res should (have size 1)
    res.head.consuming shouldBe 3

    res.head.value[ReduceRequest] should have size 2
    res.head.value[SequenceFailure] shouldBe empty
    res.head.value[PiiLog] should have size 1
  }

}
