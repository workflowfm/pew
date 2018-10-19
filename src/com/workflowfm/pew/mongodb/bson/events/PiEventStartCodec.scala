package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import com.workflowfm.pew.{PiEventStart, PiInstance}
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class PiEventStartCodec[T]( piiCodec: Codec[PiInstance[T]] )
  extends ClassCodec[PiEventStart[T]] {

  val piiN: String = "pii"
  val timeN: String = "timestamp"

  override def encodeBody(writer: BsonWriter, value: PiEventStart[T], ctx: EncoderContext): Unit = {

    writer.writeName( piiN )
    ctx.encodeWithChildContext( piiCodec, writer, value.i )
    
    writer.writeInt64( timeN, value.time )
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiEventStart[T] = {

    reader.readName( piiN )
    val pii: PiInstance[T] = ctx.decodeWithChildContext( piiCodec, reader )

    val time: Long = reader.readInt64( timeN )
    
    PiEventStart( pii, time )
  }
}
