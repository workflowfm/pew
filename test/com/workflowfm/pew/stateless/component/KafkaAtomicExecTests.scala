package com.workflowfm.pew.stateless.component

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.workflowfm.pew._
import com.workflowfm.pew.stateless.StatelessMessages.{AnyMsg, Assignment, SequenceFailure, SequenceRequest}
import com.workflowfm.pew.stateless.components.AtomicExecutor
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaWrapperFlows.{flowRespond, flowWaitFuture}
import com.workflowfm.pew.stateless.instances.kafka.components.{MockTracked, Tracked}
import com.workflowfm.pew.stateless.{CallRef, KafkaExampleTypes}
import org.bson.types.ObjectId
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.concurrent.Future

@RunWith(classOf[JUnitRunner])
class KafkaAtomicExecTests extends PewTestSuite with KafkaExampleTypes {

  val assgnException: Assignment
    = Assignment(
      PiInstance( ObjectId.get, pcif, PiObject(1) ),
      eg1.r1._1, pcif.iname,
      Seq( PiResource( PiObject(1), pai.inputs.head._1 ) )
    )

  val assgnCompositeProc: Assignment
    = Assignment(
      PiInstance( ObjectId.get, ri, PiObject(11) ),
      eg1.r1._1, ri.iname,
      Seq( PiResource( eg1.arg1, pai.inputs.head._1 ) )
    )

  val assgnUnknownProc: Assignment
    = Assignment(
      eg1.pInProgress, eg1.r1._1, ":`(",
      Seq( PiResource( eg1.arg1, pai.inputs.head._1 ) )
    )

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

  def runAEx( history: (Assignment, Int)* ): MockTracked[MessageMap] = {
    val fut: Future[ Seq[MockTracked[AnyMsg]]]
      = MockTracked
        .source( history )
        .groupBy( Int.MaxValue, _.part )
        .via( flowRespond( AtomicExecutor() ) )
        .via( flowWaitFuture( 1 )( completeProcessSettings ) )
        .mergeSubstreams
        .runWith(Sink.seq)(ActorMaterializer())

    await(
      fut
        .map( Tracked.flatten )
        .map( Tracked.fmap( new MessageMap(_) ) )
    )
  }

  it should "respond to an Assignment" in {
    val msgsOf = runAEx( (eg1.assgnInProgress, 1) )

    msgsOf.consuming shouldBe 1
    msgsOf.value[SequenceRequest] should have size 1
    msgsOf.value[SequenceFailure] shouldBe empty
  }

  it should "respond to multiple sequential Assignments" in {
    val msgsOf =
      runAEx(
        (eg1.assgnInProgress, 1),
        (eg1.assgnFinishing2, 1),
        (eg1.assgnFinishing3, 1),
      )

    msgsOf.consuming shouldBe 3
    msgsOf.value[SequenceRequest] should have size 3
    msgsOf.value[SequenceFailure] shouldBe empty
  }

  it should "respond to multiple parallel Assignments" in {
    val msgsOf =
      runAEx(
        (eg1.assgnInProgress, 1),
        (eg1.assgnFinishing2, 2),
        (eg1.assgnFinishing3, 3),
      )

    msgsOf.consuming shouldBe 3
    msgsOf.value[SequenceRequest] should have size 3
    msgsOf.value[SequenceFailure] shouldBe empty
  }

  it should "handle an exception" in {
    val msgsOf = runAEx( ( assgnException, 1) )

    msgsOf.consuming shouldBe 1
    msgsOf.value[SequenceRequest] shouldBe empty
    msgsOf.value[SequenceFailure] should have size 1
  }

  it should "continue processing Assignments after an exception" in {
    val msgsOf =
      runAEx(
        ( assgnException, 1 ),
        ( eg1.assgnInProgress, 1 ),
      )

    msgsOf.consuming shouldBe 2
    msgsOf.value[SequenceRequest] should have size 1
    msgsOf.value[SequenceFailure] should have size 1
  }

  it should "correctly respond to a CompositeProcess" in {
    val msgsOf =
      runAEx(
        ( assgnCompositeProc, 1 ),
        ( eg1.assgnInProgress, 1 ),
      )

    msgsOf.consuming shouldBe 2
    msgsOf.value[SequenceRequest] should have size 1
    msgsOf.value[SequenceFailure] should have size 1
  }

  it should "handle an unknown PiProcess" in {
    val msgsOf =
      runAEx(
        ( assgnUnknownProc, 1 ),
        ( eg1.assgnInProgress, 1 ),
      )

    msgsOf.consuming shouldBe 2
    msgsOf.value[SequenceRequest] should have size 1
    msgsOf.value[SequenceFailure] should have size 1
  }


}
