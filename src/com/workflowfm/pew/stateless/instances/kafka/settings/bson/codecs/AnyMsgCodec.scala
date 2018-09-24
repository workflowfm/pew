package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs

import com.workflowfm.pew.stateless.StatelessMessages
import com.workflowfm.pew.stateless.StatelessMessages.AnyMsg
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings._
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs._
import org.bson.codecs.configuration.CodecRegistry

class AnyMsgCodec( reg: CodecRegistry )
  extends Codec[AnyMsg] {

  import StatelessMessages._
  import PewCodecs._

  val piiUpdateN = "PiiUpdate"
  val seqReqN = "SequenceRequest"

  private val piiUpdate: Codec[PiiUpdate] = reg.get( classOf[PiiUpdate] )
  private val assgn: Codec[Assignment] = reg.get( classOf[Assignment] )
  private val seqReq: Codec[SequenceRequest] = reg.get( classOf[SequenceRequest] )
  private val redReq: Codec[ReduceRequest] = reg.get( classOf[ReduceRequest] )
  private val result: Codec[PiiResult[AnyRes]] = reg.get( classOf[PiiResult[AnyRes]] )

  override def encode(writer: BsonWriter, value: AnyMsg, ctx: EncoderContext): Unit = {
    value match {
      case m: PiiUpdate       => piiUpdate.encode( writer, m, ctx )
      case m: SequenceRequest => seqReq.encode( writer, m, ctx )
      case m: ReduceRequest   => redReq.encode( writer, m, ctx )
      case m: Assignment      => assgn.encode( writer, m, ctx )
      case m: PiiResult[_]    => result.encode( writer, m.asInstanceOf[PiiResult[AnyRes]], ctx )
      case _ =>
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
    else if ( typeName == seqReqN )
      ctx.decodeWithChildContext( seqReq, reader )
    else {
      // Only PiiHistory messages need decoding so far, as they are
      // the only 2 that are read in heterogeneous topics.
      throw UnrecognisedAnyMsg("Unrecognised AnyMsg: " + typeName)
      null
    }

  }

  override def getEncoderClass: Class[AnyMsg] = ANY_MSG
}
