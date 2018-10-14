package com.workflowfm.pew.stateless.component

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.stateless.components.AtomicExecutor
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaWrapperFlows.flowSequencer
import com.workflowfm.pew.stateless.instances.kafka.components.MockTracked
import com.workflowfm.pew.stateless.{CallRef, KafkaTests}
import com.workflowfm.pew.{PiInstance, PiItem, PiObject}
import org.bson.types.ObjectId
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

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
      seqreq(p1, 1)
    )

    res should have size 1
    res.head.value should have size 1
    res.head.consuming shouldBe 2
  }

  it should "sequence 2 PiiUpdates from different Piis and 1 SequenceRequest" in {
    val res = runSequencer(
      update(p1, 1),
      update(p2, 1),
      seqreq(p1, 1)
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
      seqreq(p1, 1),
    )

    res should (have size 1)
    res.head.value should (have size 2)
    res.head.consuming shouldBe 5
  }

  it should "sequence only 1 of 2 PiiUpdates on different partitions with 1 SequenceRequest" in {
    val res = runSequencer(
      update(p1, 1),
      update(p2, 2),
      seqreq(p1, 1),
    )

    res should (have size 1)
    res.head.value should (have size 1)
    res.head.consuming shouldBe 2
  }

  it should "not sequence a PiiUpdate and SequenceRequest for different Piis" in {
    runSequencer(
      update(p1, 1),
      seqreq(p2, 1),

    ) shouldBe empty
  }

  it should "not sequence a PiiUpdate and SequenceRequest on different partitions" in {
    runSequencer(
      update(p1, 1),
      seqreq(p1, 2),

    ) shouldBe empty
  }


}