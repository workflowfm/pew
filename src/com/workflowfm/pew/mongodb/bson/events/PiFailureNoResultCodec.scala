package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.{PiFailureNoResult, PiInstance}
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class PiFailureNoResultCodec[T]( piiCodec: Codec[PiInstance[T]] )
  extends Codec[PiFailureNoResult[T]] {

  val piiN: String = "pii"

  override def encode(writer: BsonWriter, value: PiFailureNoResult[T], ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    writer.writeName( piiN )
    ctx.encodeWithChildContext( piiCodec, writer, value.i )

    writer.writeEndDocument()
  }

  override def decode(reader: BsonReader, ctx: DecoderContext): PiFailureNoResult[T] = {
    reader.readStartDocument()

    reader.readName( piiN )
    val pii: PiInstance[T] = ctx.decodeWithChildContext( piiCodec, reader )

    reader.readEndDocument()
    PiFailureNoResult( pii )
  }

  override def getEncoderClass: Class[ PiFailureNoResult[T] ]
    = classOf[ PiFailureNoResult[T] ]
}
