package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs

import com.workflowfm.pew.stateless.StatelessMessages
import com.workflowfm.pew.stateless.instances.kafka.KafkaTopic.{AnyMsg, AnyRes}
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
  private val result: Codec[Result[AnyRes]] = reg.get( classOf[Result[AnyRes]] )

  override def encode(writer: BsonWriter, value: AnyMsg, ctx: EncoderContext): Unit = {
    value match {
      case m: PiiUpdate           => piiUpdate.encode( writer, m, ctx )
      case m: SequenceRequest     => seqReq.encode( writer, m, ctx )
      case m: ReduceRequest       => redReq.encode( writer, m, ctx )
      case m: Assignment          => assgn.encode( writer, m, ctx )
      case m: Result[_]           => result.encode( writer, m.asInstanceOf[Result[AnyRes]], ctx )
      case _ =>
    }
  }

  // Only needs to decode 2 types for PiiHistory consumers.
  override def decode(reader: BsonReader, ctx: DecoderContext): AnyMsg = {
    val mark = reader.getMark
    reader.readStartDocument()
    val name = reader.readString( "msgType" )
    mark.reset()

    if ( name == piiUpdateN )
      ctx.decodeWithChildContext( piiUpdate, reader )
    else if ( name == seqReqN )
      ctx.decodeWithChildContext( seqReq, reader )
  }

  override def getEncoderClass: Class[AnyMsg] = ANY_MSG
}
