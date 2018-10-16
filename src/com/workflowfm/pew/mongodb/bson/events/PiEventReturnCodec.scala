package com.workflowfm.pew.mongodb.bson.events

import com.workflowfm.pew.PiEventReturn
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

class PiEventReturnCodec[T]( tCodec: Codec[T], anyCodec: Codec[Any] )
  extends Codec[PiEventReturn[T]] {

  val piiN: String = "pii"
  val refN: String = "ref"
  val resN: String = "res"

  override def encode(writer: BsonWriter, value: PiEventReturn[T], ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    writer.writeName( piiN )
    ctx.encodeWithChildContext( tCodec, writer, value.id )

    writer.writeInt32( refN, value.ref )

    writer.writeName( resN )
    ctx.encodeWithChildContext( anyCodec, writer, value.result )

    writer.writeEndDocument()
  }

  override def decode(reader: BsonReader, ctx: DecoderContext): PiEventReturn[T] = {
    reader.readStartDocument()

    reader.readName( piiN )
    val id: T = ctx.decodeWithChildContext( tCodec, reader )

    val ref: Int = reader.readInt32( refN )

    reader.readName( resN )
    val result: Any = ctx.decodeWithChildContext( anyCodec, reader )

    reader.readStartDocument()
    PiEventReturn( id, ref, result )
  }

  override def getEncoderClass: Class[PiEventReturn[T]]
    = classOf[ PiEventReturn[T] ]
}
