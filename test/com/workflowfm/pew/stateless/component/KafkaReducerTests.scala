package com.workflowfm.pew.stateless.component

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.workflowfm.pew.stateless.StatelessMessages.{AnyMsg, PiiHistory, PiiUpdate, ReduceRequest}
import com.workflowfm.pew.stateless.components.Reducer
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaWrapperFlows.{flowRespond, flowSequencer}
import com.workflowfm.pew.stateless.instances.kafka.components.{MockTracked, Tracked}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.concurrent.Future

@RunWith(classOf[JUnitRunner])
class KafkaReducerTests extends KafkaComponentTests {

  def runReducer(history: (ReduceRequest, Int)*): MockTracked[MessageMap] = {
    val fut: Future[ Seq[MockTracked[Seq[AnyMsg]]] ]
      = MockTracked
        .source(history)
        .via(flowRespond(new Reducer))
        .runWith(Sink.seq)(ActorMaterializer())

    await(
      fut
      .map( Tracked.flatten )
      .map( Tracked.fmap( _.flatten ) )
      .map( Tracked.fmap( new MessageMap(_) ) )
    )
  }

  it should "handle a single reduce request" in {
    val msgsOf
      = runReducer(
          ( ReduceRequest( p1, Seq() ), 1 )
      )

    msgsOf.consuming shouldBe 1
    msgsOf.value[PiiUpdate] should have size 1
  }

}
