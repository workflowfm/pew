package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.{PiEventStart, PiInstance}
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class PiEventStartCodec[T]( piiCodec: Codec[PiInstance[T]] )
  extends Codec[PiEventStart[T]] {

  val piiN: String = "pii"

  override def encode(writer: BsonWriter, value: PiEventStart[T], ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    writer.writeName( piiN )
    ctx.encodeWithChildContext( piiCodec, writer, value.i )

    writer.writeEndDocument()
  }

  override def decode(reader: BsonReader, ctx: DecoderContext): PiEventStart[T] = {
    reader.readEndDocument()

    reader.readName( piiN )
    val pii: PiInstance[T] = ctx.decodeWithChildContext( piiCodec, reader )

    reader.readEndDocument()
    PiEventStart( pii )
  }

  override def getEncoderClass: Class[PiEventStart[T]]
    = classOf[PiEventStart[T]]
}
