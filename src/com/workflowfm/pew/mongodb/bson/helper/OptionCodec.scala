package com.workflowfm.pew.mongodb.bson.helper

import com.workflowfm.pew.mongodb.bson.auto.{ ClassCodec, SuperclassCodec }
import org.bson.{ BsonReader, BsonWriter }
import org.bson.codecs.{ Codec, DecoderContext, EncoderContext }

class NoneCodec extends ClassCodec[None.type] {
  override def decodeBody(reader: BsonReader, ctx: DecoderContext): None.type = None
  override def encodeBody(writer: BsonWriter, value: None.type, ctx: EncoderContext): Unit = {}
}

class SomeCodec(anyCodec: Codec[Any]) extends ClassCodec[Some[Any]] {

  val valueN: String = "value"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): Some[Any] = {
    reader.readName(valueN)
    Some(ctx.decodeWithChildContext(anyCodec, reader))
  }

  override def encodeBody(writer: BsonWriter, value: Some[Any], ctx: EncoderContext): Unit = {
    writer.writeName(valueN)
    ctx.encodeWithChildContext(anyCodec, writer, value.value)
  }
}

object OptionCodec {

  def apply(anyCodec: Codec[Any]): Codec[Option[Any]] = {
    val codec = new SuperclassCodec[Option[Any]]
    codec.updateWith(new NoneCodec)
    codec.updateWith(new SomeCodec(anyCodec))
    codec
  }
}
