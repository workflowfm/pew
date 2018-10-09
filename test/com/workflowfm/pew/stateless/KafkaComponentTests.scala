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

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class KafkaComponentTests extends FlatSpec with Matchers with BeforeAndAfterAll with KafkaTests {

  // Serialisation
  val reg: CodecRegistry = new KafkaCodecRegistry( completeProcessStore )

  def testCodec[T]( tOriginals: T* )( implicit codec: Codec[T] ): scalatest.Assertion = {

    val fut: Future[Seq[T]] = Future {

      val document = new BsonDocument

      // Write the source T objects to the Bson document.
      {
        val writer = new BsonDocumentWriter(document)
        val enCtx = EncoderContext.builder().build()

        var elementNum: Int = 0

        writer.writeStartDocument()
        tOriginals foreach {
          t =>
            try {
              writer.writeName( s"Element$elementNum" )
              elementNum += 1

              codec.encode(writer, t, enCtx)

            } catch {
              case e: Exception =>
                System.out.println(s"Failed to encode '$t'.")
                throw e
            }
        }
        writer.writeEndDocument()
      }

      // Read the values back from the Bson document
      val reader = new BsonDocumentReader(document)
      val deCtx = DecoderContext.builder().build()

      reader.readStartDocument()
      val out = tOriginals map {
        t =>
          try {
            reader.readName()
            codec.decode(reader, deCtx)

          } catch {
            case e: Exception =>
              System.out.println(s"Failed to decode '$t'.")
              throw e
          }
      }
      reader.readEndDocument()

      out
    }

    val tReserialized: Seq[T] = Await.result( fut, 2.seconds )
    tReserialized shouldBe tOriginals
  }

  val piInstance: PiInstance[ObjectId] = PiInstance( ObjectId.get, pbi, PiObject(1) )
  val callRes0 = ( CallRef(1), PiObject(0) )
  val callResHi = ( CallRef(0), PiObject("Hello, World!") )
  val callResErr = ( CallRef(53141), RemoteExecutorException("Argh!") )

  it should "correctly (de)serialise ReduceRequests" in {
    implicit val codec: Codec[ReduceRequest] = reg.get( classOf[ReduceRequest] )
    codec shouldNot be (null)

    val emptyReduceRequest = ReduceRequest( piInstance, Seq() )
    val singleReduceRequest = ReduceRequest( piInstance, Seq( callResHi ) )
    val multiReduceRequest = ReduceRequest( piInstance, Seq( callRes0, callResHi ) )

    testCodec( emptyReduceRequest )
    testCodec( singleReduceRequest )
    testCodec( multiReduceRequest )
    testCodec( emptyReduceRequest, singleReduceRequest, multiReduceRequest, emptyReduceRequest )
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

  it should "correctly (de)serialise a sequence of PiiHistory types" in {
    implicit val codec: Codec[PiiHistory] = reg.get( classOf[PiiHistory] )
    codec shouldNot be (null)

    testCodec[PiiHistory](
      PiiUpdate( piInstance ),
      SequenceRequest( ObjectId.get, callRes0 ),
      SequenceRequest( ObjectId.get, callResHi ),
      PiiUpdate( piInstance ),
      SequenceFailure( ObjectId.get, CallRef(1), RemoteExecutorException("Argh!") ),
      SequenceFailure( piInstance, Seq(callRes0, callResHi), Seq( callResErr ) )
    )
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
