package com.workflowfm.pew.mongodb.bson.helper

import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import org.bson.{ BsonReader, BsonWriter }
import org.bson.codecs.{ Codec, DecoderContext, EncoderContext }

class Tuple2Codec(anyCodec: Codec[Any]) extends ClassCodec[(Any, Any)] {

  val _1N: String = "_1"
  val _2N: String = "_2"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): (Any, Any) = {

    reader.readName(_1N)
    val _1 = ctx.decodeWithChildContext(anyCodec, reader)

    reader.readName(_2N)
    val _2 = ctx.decodeWithChildContext(anyCodec, reader)

    (_1, _2)
  }

  override def encodeBody(writer: BsonWriter, value: (Any, Any), ctx: EncoderContext): Unit = {

    writer.writeName(_1N)
    ctx.encodeWithChildContext(anyCodec, writer, value._1)

    writer.writeName(_2N)
    ctx.encodeWithChildContext(anyCodec, writer, value._2)
  }
}
