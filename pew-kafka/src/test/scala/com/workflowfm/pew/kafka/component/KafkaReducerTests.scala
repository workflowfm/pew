package com.workflowfm.pew.stateless.component

import scala.concurrent.Future

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import com.workflowfm.pew.PewTestSuite
import com.workflowfm.pew.kafka.KafkaExampleTypes
import com.workflowfm.pew.kafka.components.{ MockTracked, Tracked }
import com.workflowfm.pew.kafka.components.KafkaWrapperFlows.flowRespond
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.stateless.components.Reducer

@RunWith(classOf[JUnitRunner])
class KafkaReducerTests extends PewTestSuite with KafkaExampleTypes {

  def runReducer(history: (ReduceRequest, Int)*): MockTracked[MessageMap] = {
    val fut: Future[Seq[MockTracked[Seq[AnyMsg]]]] = MockTracked
      .source(history)
      .via(flowRespond(new Reducer))
      .runWith(Sink.seq)(ActorMaterializer())

    await(
      fut
        .map(Tracked.flatten)
        .map(Tracked.fmap(_.flatten))
        .map(Tracked.fmap(new MessageMap(_)))
    )
  }

  it should "handle a ReduceRequest for a new PiInstance" in {
    val msgsOf = runReducer(
      (ReduceRequest(eg1.pNew, Seq()), 1)
    )

    msgsOf.consuming shouldBe 1
    msgsOf.value[PiiUpdate] should have size 1
    msgsOf.value[Assignment] should have size 1
    msgsOf.value[PiiLog] should have size 1

    // msgsOf.value[PiiUpdate].head.pii.state.calls should have size 1
  }

  it should "handle a ReduceRequest with intermediate results" in {
    val msgsOf = runReducer(
      (ReduceRequest(eg1.pInProgress, Seq(eg1.r1)), 1)
    )

    msgsOf.consuming shouldBe 1
    msgsOf.value[PiiUpdate] should have size 1
    msgsOf.value[Assignment] should have size 2
    msgsOf.value[PiiLog] should have size 2
  }

  it should "handle a ReduceRequest completing a PiInstance" in {
    val msgsOf = runReducer(
      (ReduceRequest(eg1.pFinishing, Seq(eg1.r2, eg1.r3)), 1)
    )

    msgsOf.consuming shouldBe 1
    msgsOf.value[PiiUpdate] shouldBe empty
    msgsOf.value[Assignment] shouldBe empty
    msgsOf.value[PiiLog] should have size 1
  }

  it should "handle an irreducible PiInstance in a ReduceRequest" in {
    val msgsOf = runReducer(
      (ReduceRequest(eg1.pInProgress, Seq()), 1)
    )

    msgsOf.consuming shouldBe 1
    msgsOf.value[PiiUpdate] should have size 1
    msgsOf.value[Assignment] shouldBe empty
    msgsOf.value[PiiLog] shouldBe empty
  }

}
