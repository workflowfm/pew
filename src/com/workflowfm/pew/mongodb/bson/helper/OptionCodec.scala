package com.workflowfm.pew.mongodb.bson.helper

import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

class OptionCodec( anyCodec: Codec[Any] )
  extends ClassCodec[Option[Any]] {

  val definedN: String = "defined"
  val valueN: String = "value"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): Option[Any] = {

    val hasValue = reader.readBoolean( definedN )

    if ( hasValue ) {
      reader.readName( valueN )
      val value = ctx.decodeWithChildContext( anyCodec, reader )
      Some( value )

    } else None
  }

  override def encodeBody(writer: BsonWriter, option: Option[Any], ctx: EncoderContext): Unit = {

    writer.writeBoolean( definedN, option.isDefined )

    option match {
      case Some( value ) =>
        writer.writeName( valueN )
        ctx.encodeWithChildContext( anyCodec, writer, value )

      case None => // No action necessary.
    }
  }
}