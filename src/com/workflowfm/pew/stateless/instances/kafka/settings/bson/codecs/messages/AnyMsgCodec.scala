package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.messages

import com.workflowfm.pew.stateless.StatelessMessages
import com.workflowfm.pew.stateless.StatelessMessages.AnyMsg
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.PewCodecs
import org.bson.codecs._
import org.bson.codecs.configuration.CodecRegistry
import org.bson.{BsonReader, BsonWriter}

class AnyMsgCodec( reg: CodecRegistry )
  extends Codec[AnyMsg] {

  import PewCodecs._
  import StatelessMessages._

  val piiUpdateN = "PiiUpdate"
  val assgnN = "Assignment"
  val seqReqN = "SequenceRequest"
  val seqfailN = "SequenceFailure"
  val redreqN = "ReduceRequest"
  val resultN = "Result"

  private val piiUpdate: Codec[PiiUpdate] = reg.get( classOf[PiiUpdate] )
  private val assgn: Codec[Assignment] = reg.get( classOf[Assignment] )
  private val seqReq: Codec[SequenceRequest] = reg.get( classOf[SequenceRequest] )
  private val seqfail: Codec[SequenceFailure] = reg.get( classOf[SequenceFailure] )
  private val redReq: Codec[ReduceRequest] = reg.get( classOf[ReduceRequest] )
  private val result: Codec[PiiLog] = reg.get( classOf[PiiLog] )

  override def encode(writer: BsonWriter, value: AnyMsg, ctx: EncoderContext): Unit = {
    value match {
      case m: PiiUpdate       => piiUpdate.encode( writer, m, ctx )
      case m: SequenceRequest => seqReq.encode( writer, m, ctx )
      case m: SequenceFailure => seqfail.encode( writer, m, ctx )
      case m: ReduceRequest   => redReq.encode( writer, m, ctx )
      case m: Assignment      => assgn.encode( writer, m, ctx )
      case m: PiiLog          => result.encode( writer, m, ctx )
      case m => throw new IllegalArgumentException( s"Unsupported type '${m.getClass.getSimpleName}'." )
    }
  }

  case class UnrecognisedAnyMsg( reason: String )
    extends Exception( reason )

  // Only needs to decode 2 types for PiiHistory consumers.
  override def decode(reader: BsonReader, ctx: DecoderContext): AnyMsg = {
    val mark = reader.getMark
    reader.readStartDocument()
    val typeName = reader.readString( "msgType" )
    mark.reset()

    if ( typeName == piiUpdateN )
      ctx.decodeWithChildContext( piiUpdate, reader )
    else if ( typeName == assgnN )
      ctx.decodeWithChildContext( assgn, reader )
    else if ( typeName == seqReqN )
      ctx.decodeWithChildContext( seqReq, reader )
    else if ( typeName == seqfailN )
      ctx.decodeWithChildContext( seqfail, reader )
    else if ( typeName == redreqN )
      ctx.decodeWithChildContext( redReq, reader )
    else if ( typeName == resultN )
      ctx.decodeWithChildContext( result, reader )
    else {
      // Only PiiHistory messages need decoding so far, as they are
      // the only 2 that are read in heterogeneous topics.
      throw UnrecognisedAnyMsg("Unrecognised AnyMsg: " + typeName)
      null
    }

  }

  override def getEncoderClass: Class[AnyMsg] = ANY_MSG
}
