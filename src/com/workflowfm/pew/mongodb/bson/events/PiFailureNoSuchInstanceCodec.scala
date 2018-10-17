package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.PiFailureNoSuchInstance
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class PiFailureNoSuchInstanceCodec[T]( tCodec: Codec[T] )
  extends ClassCodec[PiFailureNoSuchInstance[T]] {

  val tIdN: String = "tId"

  override def encodeBody(writer: BsonWriter, value: PiFailureNoSuchInstance[T], ctx: EncoderContext): Unit = {

    writer.writeName( tIdN )
    ctx.encodeWithChildContext( tCodec, writer, value.id )
  }

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiFailureNoSuchInstance[T] = {

    reader.readName( tIdN )
    val tId: T = ctx.decodeWithChildContext( tCodec, reader )

    PiFailureNoSuchInstance( tId )
  }
}
