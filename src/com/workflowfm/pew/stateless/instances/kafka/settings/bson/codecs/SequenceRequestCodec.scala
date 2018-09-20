package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs

import com.workflowfm.pew.PiObject
import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.StatelessMessages.SequenceRequest
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

class SequenceRequestCodec(
    refCodec: Codec[CallRef],
    objCodec: Codec[PiObject]
  ) extends Codec[SequenceRequest] {

  import PewCodecs._

  val msgTypeN = "msgType"
  val msgType = "SequenceRequest"

  val idN = "_id"
  val refN = "callRef"
  val objN = "piObj"

  override def decode(reader: BsonReader, ctx: DecoderContext): SequenceRequest = {
    reader.readStartDocument()

    reader.readString( msgTypeN )

    reader.readName( idN )
    val piiId = reader.readObjectId()

    reader.readName( refN )
    val ref: CallRef = ctx.decodeWithChildContext( refCodec, reader )

    reader.readName( objN )
    val obj: PiObject = ctx.decodeWithChildContext( objCodec, reader )

    reader.readEndDocument()
    SequenceRequest( piiId, (ref, obj) )
  }

  override def encode(writer: BsonWriter, value: SequenceRequest, ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    writer.writeString( msgTypeN, msgType )

    writer.writeName( idN )
    writer.writeObjectId( value.piiId )

    val (callRef, piObj) = value.request

    writer.writeName( refN )
    ctx.encodeWithChildContext( refCodec, writer, callRef )

    writer.writeName( objN )
    ctx.encodeWithChildContext( objCodec, writer, piObj )

    writer.writeEndDocument()
  }

  override def getEncoderClass: Class[SequenceRequest] = SEQUENCE_REQ

}