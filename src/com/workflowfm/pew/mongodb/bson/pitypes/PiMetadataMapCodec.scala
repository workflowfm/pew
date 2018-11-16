package com.workflowfm.pew.mongodb.bson.pitypes

import com.workflowfm.pew.PiMetadata.PiMetadataMap
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

class PiMetadataMapCodec( anyCodec: Codec[Any] )
  extends ClassCodec[PiMetadataMap] {

  import com.workflowfm.pew.mongodb.bson.BsonUtil._

  val fieldsN: String = "fields"
  val typeN: String = "datatype"
  val valueN: String = "value"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiMetadataMap = {
    readArray( reader, fieldsN ) {
      () =>
        val datatype: String = reader.readString( typeN )
        val value: Any = ctx.decodeWithChildContext( anyCodec, reader )

        datatype -> value
    }
  }

  override def encodeBody(writer: BsonWriter, value: PiMetadataMap, ctx: EncoderContext): Unit = {
    writeArray( writer, fieldsN, value.to ) {
      case (key, data) =>
        writer.writeString( typeN, key )
        ctx.encodeWithChildContext( anyCodec, writer, data )
    }
  }

}
