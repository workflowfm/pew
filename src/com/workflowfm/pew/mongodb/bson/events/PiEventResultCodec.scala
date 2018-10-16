package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.{PiEventResult, PiInstance}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

class PiEventResultCodec[T]( piiCodec: Codec[PiInstance[T]], anyCodec: Codec[Any] )
  extends Codec[PiEventResult[T]]{

  val piiN: String = "pii"
  val resultN: String = "result"

  override def encode(writer: BsonWriter, value: PiEventResult[T], ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    writer.writeName( piiN )
    ctx.encodeWithChildContext( piiCodec, writer, value.i )

    writer.writeName( resultN )
    ctx.encodeWithChildContext( anyCodec, writer, value.res )

    writer.writeEndDocument()
  }

  override def decode(reader: BsonReader, ctx: DecoderContext): PiEventResult[T] = {
    reader.readStartDocument()

    reader.readName( piiN )
    val pii: PiInstance[T] = ctx.decodeWithChildContext( piiCodec, reader )

    reader.readName( resultN )
    val result: Any = ctx.decodeWithChildContext( anyCodec, reader )

    reader.readEndDocument()
    PiEventResult( pii, result )
  }

  override def getEncoderClass: Class[PiEventResult[T]]
    = classOf[ PiEventResult[T] ]
}
