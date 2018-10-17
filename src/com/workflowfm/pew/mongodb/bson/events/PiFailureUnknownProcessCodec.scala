package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import com.workflowfm.pew.{PiFailureUnknownProcess, PiInstance}
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class PiFailureUnknownProcessCodec[T]( piiCodec: Codec[PiInstance[T]] )
  extends ClassCodec[PiFailureUnknownProcess[T]]{

  val piiN: String = "pii"
  val atomicProcN: String = "proc"

  override def encodeBody(writer: BsonWriter, value: PiFailureUnknownProcess[T], ctx: EncoderContext): Unit = {

    writer.writeName( piiN )
    ctx.encodeWithChildContext( piiCodec, writer, value.i )

    writer.writeString( atomicProcN, value.process )
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiFailureUnknownProcess[T] = {

    reader.readName( piiN )
    val pii: PiInstance[T] = ctx.decodeWithChildContext( piiCodec, reader )

    val proc: String = reader.readString( atomicProcN )

    PiFailureUnknownProcess( pii, proc )
  }
}
