package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs

import com.workflowfm.pew.stateless.StatelessMessages
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings.AnyRes
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.codecs.configuration.CodecRegistry

class PiiHistoryCodec( seqReq: Codec[SequenceRequest], piiUpdate: Codec[PiiUpdate] )
  extends Codec[PiiHistory] {

  import StatelessMessages._
  import PewCodecs._

  val piiUpdateN = "PiiUpdate"
  val seqReqN = "SequenceRequest"

  override def encode(writer: BsonWriter, value: PiiHistory, ctx: EncoderContext): Unit = {
    value match {
      case m: SequenceRequest => seqReq.encode( writer, m, ctx )
      case m: PiiUpdate       => piiUpdate.encode( writer, m, ctx )
      case _ =>
    }
  }

  case class UnrecognisedPiiHistoryMessage( reason: String )
    extends Exception( reason )

  // Only needs to decode 2 types for PiiHistory consumers.
  override def decode(reader: BsonReader, ctx: DecoderContext): PiiHistory = {
    val mark = reader.getMark
    reader.readStartDocument()
    val typeName = reader.readString( "msgType" )
    mark.reset()

    if ( typeName == piiUpdateN )
      ctx.decodeWithChildContext( piiUpdate, reader )
    else if ( typeName == seqReqN )
      ctx.decodeWithChildContext( seqReq, reader )
    else {
      throw UnrecognisedPiiHistoryMessage("Unrecognised PiiHistory: " + typeName)
      null
    }

  }

  override def getEncoderClass: Class[PiiHistory] = PII_HISTORY
}
