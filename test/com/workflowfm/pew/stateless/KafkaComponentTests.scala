package com.workflowfm.pew.stateless

import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings.{AnyKey, KeyPiiId, KeyPiiIdCall}
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.{CodecWrapper, KafkaCodecRegistry}
import com.workflowfm.pew.{PiInstance, PiObject}
import org.bson.codecs.Codec
import org.bson.codecs.configuration.CodecRegistry
import org.bson.types.ObjectId
import org.junit.runner.RunWith
import org.scalatest
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.reflect.ClassTag

@RunWith(classOf[JUnitRunner])
class KafkaComponentTests extends FlatSpec with Matchers with BeforeAndAfterAll with KafkaTests {

  // Serialisation
  val registry: CodecRegistry = new KafkaCodecRegistry( completeProcessStore )

  def testCodec[T]( tOriginal: T )( implicit ct: ClassTag[T], codec: Codec[T] ): scalatest.Assertion = {

    val wrapper = CodecWrapper[T]( ct, registry )
    val bytes = wrapper.serialize( "FakeTopic", tOriginal )
    val tReserialized = wrapper.deserialize( "FakeTopic", bytes )

    tReserialized shouldBe tOriginal
  }

  val piInstance: PiInstance[ObjectId] = PiInstance( ObjectId.get, pbi, PiObject(1) )
  val callRes0 = ( CallRef(1), PiObject(0) )
  val callResHi = ( CallRef(0), PiObject("Hello, World!") )
  val callResErr = ( CallRef(53141), RemoteExecutorException("Argh!") )

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

  // TODO: Ignoring for now as AnyKey can't deserialise yet and it doesn't need to.
  ignore should "correctly (de)serialise AnyKeys" in {
    implicit val codec: Codec[AnyKey] = registry.get( classOf[AnyKey] )
    codec shouldNot be (null)

    testCodec[AnyKey]( KeyPiiId( ObjectId.get ) )
    testCodec[AnyKey]( KeyPiiIdCall( ObjectId.get, CallRef(0) ) )
    testCodec[AnyKey]( KeyPiiIdCall( ObjectId.get, CallRef(1) ) )
  }

  it should "correctly (de)serialise ReduceRequests" in {
    implicit val codec: Codec[ReduceRequest] = registry.get( classOf[ReduceRequest] )
    codec shouldNot be (null)

    val emptyReduceRequest = ReduceRequest( piInstance, Seq() )
    val singleReduceRequest = ReduceRequest( piInstance, Seq( callResHi ) )
    val multiReduceRequest = ReduceRequest( piInstance, Seq( callRes0, callResHi ) )

    testCodec( emptyReduceRequest )
    testCodec( singleReduceRequest )
    testCodec( multiReduceRequest )
  }

  it should "correctly (de)serialise SequenceRequests" in {
    implicit val codec: Codec[SequenceRequest] = registry.get( classOf[SequenceRequest] )
    codec shouldNot be (null)

    testCodec( SequenceRequest( ObjectId.get, callRes0 ) )
    testCodec( SequenceRequest( ObjectId.get, callResHi ) )
  }

  it should "correctly (de)serialise SequenceFailures" in {
    implicit val codec: Codec[SequenceFailure] = registry.get( classOf[SequenceFailure] )
    codec shouldNot be (null)

    testCodec( SequenceFailure( ObjectId.get, CallRef(1), RemoteExecutorException("Argh!") ) )
    testCodec( SequenceFailure( piInstance, Seq(), Seq() ) )
    testCodec( SequenceFailure( piInstance, Seq(callRes0, callResHi), Seq( callResErr ) ) )
    testCodec( SequenceFailure( piInstance, Seq(), Seq( callResErr, callResErr, callResErr ) ) )
  }

  it should "correctly (de)serialise PiiUpdates" in {
    implicit val codec: Codec[PiiUpdate] = registry.get( classOf[PiiUpdate] )
    codec shouldNot be (null)

    testCodec( PiiUpdate( piInstance ) )
  }

  it should "correctly (de)serialise mixed PiiHistory types" in {
    implicit val codec: Codec[PiiHistory] = registry.get( classOf[PiiHistory] )
    codec shouldNot be (null)

    testCodec[PiiHistory]( PiiUpdate( piInstance ) )
    testCodec[PiiHistory]( SequenceRequest( ObjectId.get, callRes0 ) )
    testCodec[PiiHistory]( SequenceRequest( ObjectId.get, callResHi ) )
    testCodec[PiiHistory]( SequenceFailure( ObjectId.get, CallRef(1), RemoteExecutorException("Argh!") ) )
    testCodec[PiiHistory]( SequenceFailure( piInstance, Seq(callRes0, callResHi), Seq( callResErr ) ) )
  }

  it should "correctly (de)serialise Assignments" in {
    implicit val codec: Codec[Assignment] = registry.get( classOf[Assignment] )
    codec shouldNot be (null)

    testCodec( Assignment( piInstance, CallRef(0), "Pb", Seq() ) )
  }

  it should "correctly (de)serialise PiiResult" in {
    implicit val codec: Codec[PiiResult[Any]] = registry.get( classOf[PiiResult[Any]] )
    codec shouldNot be (null)

    testCodec( PiiResult[Any]( piInstance, PiObject( 1 ) ) )
    testCodec( PiiResult[Any]( piInstance, PiObject( "Hello, World!" ) ) )
    testCodec( PiiResult[Any]( piInstance, RemoteExecutorException("Argh!") ) )
  }

  it should "correctly (de)serialise mixed AnyMsgs" in {
    implicit val codec: Codec[AnyMsg] = registry.get( classOf[AnyMsg] )
    codec shouldNot be (null)

    testCodec[AnyMsg]( ReduceRequest( piInstance, Seq() ) )
    testCodec[AnyMsg]( ReduceRequest( piInstance, Seq( callResHi ) ) )
    testCodec[AnyMsg]( ReduceRequest( piInstance, Seq( callRes0, callResHi ) ) )

    testCodec[AnyMsg]( PiiUpdate( piInstance ) )

    testCodec[AnyMsg]( SequenceRequest( ObjectId.get, callRes0 ) )
    testCodec[AnyMsg]( SequenceRequest( ObjectId.get, callResHi ) )

    testCodec[AnyMsg]( SequenceFailure( ObjectId.get, CallRef(1), RemoteExecutorException("Argh!") ) )
    testCodec[AnyMsg]( SequenceFailure( piInstance, Seq(callRes0, callResHi), Seq( callResErr ) ) )

    testCodec[AnyMsg]( Assignment( piInstance, CallRef(0), "Pb", Seq() ) )

    testCodec[AnyMsg]( PiiResult( piInstance, PiObject( 1 ) ) )
    testCodec[AnyMsg]( PiiResult( piInstance, PiObject( "Hello, World!" ) ) )
    testCodec[AnyMsg]( PiiResult( piInstance, RemoteExecutorException("Argh!") ) )
  }
}
