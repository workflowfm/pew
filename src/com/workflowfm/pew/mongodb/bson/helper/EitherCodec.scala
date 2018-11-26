package com.workflowfm.pew.mongodb.bson.helper

import com.workflowfm.pew.mongodb.bson.auto.{ClassCodec, SuperclassCodec}
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class LeftCodec( anyCodec: Codec[Any] )
  extends ClassCodec[Left[Any, Any]] {

  val valueN: String = "value"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): Left[Any,Any] = {
    reader.readName( valueN )
    Left( ctx.decodeWithChildContext( anyCodec, reader ) )
  }

  override def encodeBody(writer: BsonWriter, value: Left[Any,Any], ctx: EncoderContext): Unit = {
    writer.writeName( valueN )
    ctx.encodeWithChildContext( anyCodec, writer, value.value )
  }
}

class RightCodec( anyCodec: Codec[Any] )
  extends ClassCodec[Right[Any, Any]] {

  val valueN: String = "value"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): Right[Any,Any] = {
    reader.readName( valueN )
    Right( ctx.decodeWithChildContext( anyCodec, reader ) )
  }

  override def encodeBody(writer: BsonWriter, value: Right[Any,Any], ctx: EncoderContext): Unit = {
    writer.writeName( valueN )
    ctx.encodeWithChildContext( anyCodec, writer, value.value )
  }
}

object EitherCodec {
  def apply( anyCodec: Codec[Any] ): Codec[Either[Any, Any]] = {
    val codec = new SuperclassCodec[Either[Any, Any]]
    codec.updateWith( new LeftCodec( anyCodec ) )
    codec.updateWith( new RightCodec( anyCodec ) )
    codec
  }
}