package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.messages

import com.workflowfm.pew.PiInstance
import com.workflowfm.pew.stateless.StatelessMessages.PiiUpdate
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.PewCodecs._
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.types.ObjectId
import org.bson.{BsonReader, BsonWriter}

class PiiUpdateCodec( piiCodec: Codec[PiiT] )
  extends Codec[PiiUpdate] {

  val msgTypeN = "msgType"
  val msgType = "PiiUpdate"

  val piiN = "pii"

  override def decode(reader: BsonReader, ctx: DecoderContext): PiiUpdate = {
    reader.readStartDocument()

    reader.readString( msgTypeN )

    reader.readName( piiN )
    val piiVal: PiInstance[ObjectId] = ctx.decodeWithChildContext( piiCodec, reader )

    reader.readEndDocument()
    PiiUpdate( piiVal )
  }

  override def encode(writer: BsonWriter, value: PiiUpdate, ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    writer.writeString( msgTypeN, msgType )

    writer.writeName( piiN )
    ctx.encodeWithChildContext( piiCodec, writer, value.pii )

    writer.writeEndDocument()
  }

  override def getEncoderClass: Class[PiiUpdate] = PII_UPDATE

}