package com.workflowfm.pew.stateless

import com.workflowfm.pew._
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings.{AnyKey, KeyPiiId, KeyPiiIdCall}
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.CodecWrapper
import org.bson.codecs.Codec
import org.bson.codecs.configuration.CodecRegistry
import org.bson.types.ObjectId
import org.junit.runner.RunWith
import org.scalatest
import org.scalatest.junit.JUnitRunner

import scala.reflect.ClassTag

@RunWith(classOf[JUnitRunner])
class KafkaCodecTests extends PewTestSuite with KafkaExampleTypes {

  val registry: CodecRegistry = completeProcess.settings.reg

  def testCodec[T]( tOriginal: T )( implicit ct: ClassTag[T], codec: Codec[T] ): scalatest.Assertion = {

    val wrapper = CodecWrapper[T]( ct, registry )
    val bytes = wrapper.serialize( "FakeTopic", tOriginal )
    val tReserialized = wrapper.deserialize( "FakeTopic", bytes )

    tReserialized shouldEqual tOriginal
  }

  it should "correctly (de)serialise KeyPiiIds" in {
    implicit val codec: Codec[KeyPiiId] = registry.get( classOf[KeyPiiId] )
    codec shouldNot be (null)

    testCodec( KeyPiiId( ObjectId.get ) )
  }

  it should "correctly (de)serialise KeyPiiIdCall" in {
    implicit val codec: Codec[KeyPiiIdCall] = registry.get( classOf[KeyPiiIdCall] )
    codec shouldNot be (null)

    testCodec( KeyPiiIdCall( ObjectId.get, CallRef(0) ) )
    testCodec( KeyPiiIdCall( ObjectId.get, CallRef(1) ) )
  }

  it should "correctly (de)serialise AnyKeys" in {
    implicit val codec: Codec[AnyKey] = registry.get( classOf[AnyKey] )
    codec shouldNot be (null)

    testCodec[AnyKey]( KeyPiiId( ObjectId.get ) )
    testCodec[AnyKey]( KeyPiiIdCall( ObjectId.get, CallRef(0) ) )
    testCodec[AnyKey]( KeyPiiIdCall( ObjectId.get, CallRef(1) ) )
  }

  it should "correctly (de)serialise ReduceRequests" in {
    implicit val codec: Codec[ReduceRequest] = registry.get( classOf[ReduceRequest] )
    codec shouldNot be (null)

    testCodec( eg1.rrNew )
    testCodec( eg1.rrInProgress )
    testCodec( eg1.rrFinishing2 )
    testCodec( eg1.rrFinishing3 )
    testCodec( eg1.rrFinishing23 )
  }

  it should "correctly (de)serialise SequenceRequests" in {
    implicit val codec: Codec[SequenceRequest] = registry.get( classOf[SequenceRequest] )
    codec shouldNot be (null)

    testCodec( eg1.srInProgress )
    testCodec( eg1.srFinishing2 )
    testCodec( eg1.srFinishing3 )
  }

  it should "correctly (de)serialise SequenceFailures" in {
    implicit val codec: Codec[SequenceFailure] = registry.get( classOf[SequenceFailure] )
    codec shouldNot be (null)

    testCodec( eg1.sfInProgress )
    testCodec( eg1.sfFinishing21 )
    testCodec( eg1.sfFinishing22 )
    testCodec( eg1.sfFinishing3 )

    testCodec( SequenceFailure( Left( eg1.piiId ), Seq(), Seq() ) )
    testCodec( SequenceFailure( Right( eg1.pInProgress ), Seq(), Seq() ) )
  }

  it should "correctly (de)serialise PiiUpdates" in {
    implicit val codec: Codec[PiiUpdate] = registry.get( classOf[PiiUpdate] )
    codec shouldNot be (null)

    testCodec( PiiUpdate( eg1.pNew ) )
    testCodec( PiiUpdate( eg1.pInProgress ) )
    testCodec( PiiUpdate( eg1.pFinishing ) )
    testCodec( PiiUpdate( eg1.pCompleted ) )
  }

  it should "correctly (de)serialise mixed PiiHistory types" in {
    implicit val codec: Codec[PiiHistory] = registry.get( classOf[PiiHistory] )
    codec shouldNot be (null)

    testCodec[PiiHistory]( PiiUpdate( eg1.pNew ) )
    testCodec[PiiHistory]( PiiUpdate( eg1.pInProgress ) )
    testCodec[PiiHistory]( PiiUpdate( eg1.pFinishing ) )
    testCodec[PiiHistory]( PiiUpdate( eg1.pCompleted ) )

    testCodec[PiiHistory]( eg1.srInProgress )
    testCodec[PiiHistory]( eg1.srFinishing2 )
    testCodec[PiiHistory]( eg1.srFinishing3 )

    testCodec[PiiHistory]( eg1.sfInProgress )
    testCodec[PiiHistory]( eg1.sfFinishing21 )
    testCodec[PiiHistory]( eg1.sfFinishing22 )
    testCodec[PiiHistory]( eg1.sfFinishing3 )
  }

  it should "correctly (de)serialise Assignments" in {
    implicit val codec: Codec[Assignment] = registry.get( classOf[Assignment] )
    codec shouldNot be (null)

    testCodec( eg1.assgnInProgress )
    testCodec( eg1.assgnFinishing2 )
    testCodec( eg1.assgnFinishing3 )
  }

  it should "correctly (de)serialise PiiLogs" in {
    implicit val codec: Codec[PiiLog] = registry.get( classOf[PiiLog] )
    codec shouldNot be (null)

    testCodec( PiiLog( PiEventStart( eg1.pNew ) ) )

    testCodec( PiiLog( PiEventResult( eg1.pCompleted, callRes0._2 ) ) )
    testCodec( PiiLog( PiEventResult( eg1.pCompleted, callResHi._2 ) ) )

    testCodec( PiiLog( PiEventCall( eg1.piiId, eg1.r1._1.id, eg1.proc1, Seq( eg1.arg1 ) ) ) )
    testCodec( PiiLog( PiEventCall( eg1.piiId, eg1.r2._1.id, eg1.proc2, Seq( eg1.arg2 ) ) ) )
    testCodec( PiiLog( PiEventCall( eg1.piiId, eg1.r3._1.id, eg1.proc3, Seq( eg1.arg3 ) ) ) )

    testCodec( PiiLog( PiEventReturn( eg1.piiId, eg1.r1._1.id, eg1.r1._2 ) ) )
    testCodec( PiiLog( PiEventReturn( eg1.piiId, eg1.r2._1.id, eg1.r2._2 ) ) )
    testCodec( PiiLog( PiEventReturn( eg1.piiId, eg1.r3._1.id, eg1.r3._2 ) ) )

    testCodec( PiiLog( PiFailureNoResult( eg1.pNew ) ) )
    testCodec( PiiLog( PiFailureUnknownProcess( eg1.pInProgress, "SumPr0ssess" ) ) )
    testCodec( PiiLog( PiFailureAtomicProcessIsComposite( eg1.pFinishing, "SomeCompositeProc" ) ) )
    testCodec( PiiLog( PiFailureNoSuchInstance( eg1.piiId ) ) )
    testCodec( PiiLog( PiEventException( eg1.piiId, testException ) ) )
    testCodec( PiiLog( PiEventProcessException( eg1.piiId, eg1.r1._1.id, testException ) ) )
  }

  it should "correctly (de)serialise mixed AnyMsgs" in {
    implicit val codec: Codec[AnyMsg] = registry.get( classOf[AnyMsg] )
    codec shouldNot be (null)

    testCodec[AnyMsg]( eg1.rrNew )
    testCodec[AnyMsg]( eg1.rrInProgress )
    testCodec[AnyMsg]( eg1.rrFinishing2 )
    testCodec[AnyMsg]( eg1.rrFinishing3 )
    testCodec[AnyMsg]( eg1.rrFinishing23 )

    testCodec[AnyMsg]( PiiUpdate( eg1.pNew ) )
    testCodec[AnyMsg]( PiiUpdate( eg1.pInProgress ) )
    testCodec[AnyMsg]( PiiUpdate( eg1.pFinishing ) )
    testCodec[AnyMsg]( PiiUpdate( eg1.pCompleted ) )

    testCodec[AnyMsg]( eg1.srInProgress )
    testCodec[AnyMsg]( eg1.srFinishing2 )
    testCodec[AnyMsg]( eg1.srFinishing3 )

    testCodec[AnyMsg]( eg1.sfInProgress )
    testCodec[AnyMsg]( eg1.sfFinishing21 )
    testCodec[AnyMsg]( eg1.sfFinishing22 )
    testCodec[AnyMsg]( eg1.sfFinishing3 )

    testCodec[AnyMsg]( eg1.assgnInProgress )
    testCodec[AnyMsg]( eg1.assgnFinishing2 )
    testCodec[AnyMsg]( eg1.assgnFinishing3 )

    testCodec[AnyMsg]( PiiLog( PiEventStart( eg1.pNew ) ) )
    testCodec[AnyMsg]( PiiLog( PiEventResult( eg1.pCompleted, callRes0._2 ) ) )
    testCodec[AnyMsg]( PiiLog( PiEventResult( eg1.pCompleted, callResHi._2 ) ) )
    testCodec[AnyMsg]( PiiLog( PiEventCall( eg1.piiId, eg1.r1._1.id, eg1.proc1, Seq( eg1.arg1 ) ) ) )
    testCodec[AnyMsg]( PiiLog( PiEventCall( eg1.piiId, eg1.r2._1.id, eg1.proc2, Seq( eg1.arg2 ) ) ) )
    testCodec[AnyMsg]( PiiLog( PiEventCall( eg1.piiId, eg1.r3._1.id, eg1.proc3, Seq( eg1.arg3 ) ) ) )
    testCodec[AnyMsg]( PiiLog( PiEventReturn( eg1.piiId, eg1.r1._1.id, eg1.r1._2 ) ) )
    testCodec[AnyMsg]( PiiLog( PiEventReturn( eg1.piiId, eg1.r2._1.id, eg1.r2._2 ) ) )
    testCodec[AnyMsg]( PiiLog( PiEventReturn( eg1.piiId, eg1.r3._1.id, eg1.r3._2 ) ) )
    testCodec[AnyMsg]( PiiLog( PiFailureNoResult( eg1.pNew ) ) )
    testCodec[AnyMsg]( PiiLog( PiFailureUnknownProcess( eg1.pInProgress, "SumPr0ssess" ) ) )
    testCodec[AnyMsg]( PiiLog( PiFailureAtomicProcessIsComposite( eg1.pFinishing, "SomeCompositeProc" ) ) )
    testCodec[AnyMsg]( PiiLog( PiFailureNoSuchInstance( eg1.piiId ) ) )
    testCodec[AnyMsg]( PiiLog( PiEventException( eg1.piiId, testException ) ) )
    testCodec[AnyMsg]( PiiLog( PiEventProcessException( eg1.piiId, eg1.r1._1.id, testException ) ) )
  }
}
