package com.workflowfm.pew.mongodb.bson.helper

import org.bson.codecs.{ Codec, DecoderContext, EncoderContext }
import org.bson.types.ObjectId
import org.bson.{ BsonReader, BsonWriter }

class ObjectIdCodec extends Codec[ObjectId] {

  override def encode(writer: BsonWriter, value: ObjectId, ctx: EncoderContext): Unit =
    writer.writeObjectId(value)

  override def decode(reader: BsonReader, ctx: DecoderContext): ObjectId = reader.readObjectId()

  override def getEncoderClass: Class[ObjectId] = classOf[ObjectId]
}
