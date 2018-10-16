package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.PiFailureNoSuchInstance
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class PiFailureNoSuchInstanceCodec[T]( tCodec: Codec[T] )
  extends Codec[PiFailureNoSuchInstance[T]] {

  val tIdN: String = "tId"

  override def encode(writer: BsonWriter, value: PiFailureNoSuchInstance[T], ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    writer.writeName( tIdN )
    ctx.encodeWithChildContext( tCodec, writer, value.id )

    writer.writeEndDocument()
  }

  override def decode(reader: BsonReader, ctx: DecoderContext): PiFailureNoSuchInstance[T] = {
    reader.readStartDocument()

    reader.readName( tIdN )
    val tId: T = ctx.decodeWithChildContext( tCodec, reader )

    reader.readEndDocument()
    PiFailureNoSuchInstance( tId )
  }

  override def getEncoderClass: Class[PiFailureNoSuchInstance[T]]
    = classOf[PiFailureNoSuchInstance[T]]
}
