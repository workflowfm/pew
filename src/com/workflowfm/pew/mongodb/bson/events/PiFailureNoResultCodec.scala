package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import com.workflowfm.pew.{PiFailureNoResult, PiInstance}
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class PiFailureNoResultCodec[T]( piiCodec: Codec[PiInstance[T]] )
  extends ClassCodec[PiFailureNoResult[T]] {

  val piiN: String = "pii"

  override def encodeBody(writer: BsonWriter, value: PiFailureNoResult[T], ctx: EncoderContext): Unit = {
    writer.writeName( piiN )
    ctx.encodeWithChildContext( piiCodec, writer, value.i )
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiFailureNoResult[T] = {

    reader.readName( piiN )
    val pii: PiInstance[T] = ctx.decodeWithChildContext( piiCodec, reader )

    PiFailureNoResult( pii )
  }
}
