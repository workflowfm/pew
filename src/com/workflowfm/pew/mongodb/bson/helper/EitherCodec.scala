package com.workflowfm.pew.mongodb.bson.helper

import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class EitherCodec( anyCodec: Codec[Any] )
  extends ClassCodec[Either[Any,Any]] {

  val leftN: String = "l"
  val rightN: String = "r"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): Either[Any,Any] = {
    val name: String = reader.readName()
    val value: Any = ctx.decodeWithChildContext( anyCodec, reader )

    if (name == leftN) Left( value )
    else if (name == rightN) Right( value )
    else throw new Exception(s"Unrecognised fieldname '$name'.")
  }

  override def encodeBody(writer: BsonWriter, value: Either[Any,Any], ctx: EncoderContext): Unit = {
    value match {
      case Left( v ) =>
        writer.writeName(leftN)
        ctx.encodeWithChildContext( anyCodec, writer, v )

      case Right( v ) =>
        writer.writeName(rightN)
        ctx.encodeWithChildContext( anyCodec, writer, v )
    }
  }
}