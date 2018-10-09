package com.workflowfm.pew.stateless

import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.KafkaCodecRegistry
import com.workflowfm.pew.{PiInstance, PiObject}
import org.bson.codecs.configuration.CodecRegistry
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.types.ObjectId
import org.bson.{BsonDocument, BsonDocumentReader, BsonDocumentWriter}
import org.junit.runner.RunWith
import org.scalatest
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class KafkaComponentTests extends FlatSpec with Matchers with BeforeAndAfterAll with KafkaTests {

  // Serialisation
  val reg: CodecRegistry = new KafkaCodecRegistry( completeProcessStore )


  def testCodec[T]( tOriginal: T )( implicit codec: Codec[T] ): scalatest.Assertion = {

    val document = new BsonDocument

    codec.encode(
      new BsonDocumentWriter(document),
      tOriginal,
      EncoderContext.builder().build()
    )

    val tReserialised: T
      = codec.decode(
        new BsonDocumentReader(document),
        DecoderContext.builder().build()
      )

    tReserialised shouldBe tOriginal
  }

  val piInstance: PiInstance[ObjectId] = PiInstance( ObjectId.get, pbi, PiObject(1) )
  val callRes0 = ( CallRef(1), PiObject(0) )
  val callResHi = ( CallRef(0), PiObject("Hello, World!") )
  val callResErr = ( CallRef(53141), RemoteExecutorException("Argh!") )

  it should "correctly (de)serialise ReduceRequests" in {
    implicit val codec: Codec[ReduceRequest] = reg.get( classOf[ReduceRequest] )
    codec shouldNot be (null)

    testCodec( ReduceRequest( piInstance, Seq() ) )
    testCodec( ReduceRequest( piInstance, Seq( callResHi ) ) )
  }

  it should "correctly (de)serialise SequenceRequests" in {
    implicit val codec: Codec[SequenceRequest] = reg.get( classOf[SequenceRequest] )
    codec shouldNot be (null)

    testCodec( SequenceRequest( ObjectId.get, callRes0 ) )
    testCodec( SequenceRequest( ObjectId.get, callResHi ) )
  }

  it should "correctly (de)serialise SequenceFailures" in {
    implicit val codec: Codec[SequenceFailure] = reg.get( classOf[SequenceFailure] )
    codec shouldNot be (null)

    testCodec( SequenceFailure( ObjectId.get, CallRef(1), RemoteExecutorException("Argh!") ) )
    testCodec( SequenceFailure( piInstance, Seq(), Seq() ) )
    testCodec( SequenceFailure( piInstance, Seq(callRes0, callResHi), Seq( callResErr ) ) )
    testCodec( SequenceFailure( piInstance, Seq(), Seq( callResErr, callResErr, callResErr ) ) )
  }

  it should "correctly (de)serialise PiiUpdates" in {
    implicit val codec: Codec[PiiUpdate] = reg.get( classOf[PiiUpdate] )
    codec shouldNot be (null)

    testCodec( PiiUpdate( piInstance ) )
  }

  it should "correctly (de)serialise Assignments" in {
    implicit val codec: Codec[Assignment] = reg.get( classOf[Assignment] )
    codec shouldNot be (null)

    testCodec( Assignment( piInstance, CallRef(0), "Pb", Seq() ) )
  }

  it should "correctly (de)serialise PiiResult" in {
    implicit val codec: Codec[PiiResult[Any]] = reg.get( classOf[PiiResult[Any]] )
    codec shouldNot be (null)

    testCodec( PiiResult[Any]( piInstance, PiObject( 1 ) ) )
    testCodec( PiiResult[Any]( piInstance, PiObject( "Hello, World!" ) ) )
    testCodec( PiiResult[Any]( piInstance, RemoteExecutorException("Argh!") ) )
  }
}
