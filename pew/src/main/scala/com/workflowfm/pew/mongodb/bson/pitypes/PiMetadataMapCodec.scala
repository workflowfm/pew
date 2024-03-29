package com.workflowfm.pew.mongodb.bson.pitypes

import org.bson.{ BsonReader, BsonWriter }
import org.bson.codecs.{ Codec, DecoderContext, EncoderContext }

import com.workflowfm.pew.PiMetadata.PiMetadataMap
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec

class PiMetadataMapCodec(anyCodec: Codec[Any]) extends ClassCodec[PiMetadataMap] {

  import com.workflowfm.pew.mongodb.bson.BsonUtil._

  val fieldsN: String = "fields"
  val typeN: String = "datatype"
  val valueN: String = "value"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiMetadataMap = {
    readArray(reader, fieldsN) { () =>
      reader.readStartDocument()
      val datatype: String = reader.readString(typeN)

      reader.readName(valueN)
      val value: Any = ctx.decodeWithChildContext(anyCodec, reader)
      reader.readEndDocument()

      datatype -> value
    }
  }

  override def encodeBody(writer: BsonWriter, value: PiMetadataMap, ctx: EncoderContext): Unit = {
    writeArray(writer, fieldsN, value.to) {
      case (key, data) =>
        writer.writeStartDocument()
        writer.writeString(typeN, key)

        writer.writeName(valueN)
        ctx.encodeWithChildContext(anyCodec, writer, data)
        writer.writeEndDocument()
    }
  }

}
