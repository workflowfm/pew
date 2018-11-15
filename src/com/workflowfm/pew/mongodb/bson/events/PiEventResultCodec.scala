package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import com.workflowfm.pew.{PiEventResult, PiInstance, PiTimes}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

class PiEventResultCodec[T]( piiCodec: Codec[PiInstance[T]], anyCodec: Codec[Any], timeCodec: Codec[PiTimes] )
  extends ClassCodec[PiEventResult[T]]{

  val piiN: String = "pii"
  val resultN: String = "result"
  val timeN: String = "timestamp"

  override def encodeBody(writer: BsonWriter, value: PiEventResult[T], ctx: EncoderContext): Unit = {

    writer.writeName( piiN )
    ctx.encodeWithChildContext( piiCodec, writer, value.i )

    writer.writeName( resultN )
    ctx.encodeWithChildContext( anyCodec, writer, value.res )

    writer.writeName( timeN )
    ctx.encodeWithChildContext( timeCodec, writer, value.times )
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiEventResult[T] = {

    reader.readName( piiN )
    val pii: PiInstance[T] = ctx.decodeWithChildContext( piiCodec, reader )

    reader.readName( resultN )
    val result: Any = ctx.decodeWithChildContext( anyCodec, reader )

    reader.readName( timeN )
    val time: PiTimes = ctx.decodeWithChildContext( timeCodec, reader )
    
    PiEventResult( pii, result, time )
  }
}
