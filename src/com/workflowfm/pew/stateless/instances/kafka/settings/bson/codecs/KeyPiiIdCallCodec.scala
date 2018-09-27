package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs

import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings.KeyPiiIdCall
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

class KeyPiiIdCallCodec( refCodec: Codec[CallRef] )
  extends Codec[KeyPiiIdCall] {

  import PewCodecs._

  val idN = "_id"
  val refN = "ref"

  override def decode(reader: BsonReader, ctx: DecoderContext): KeyPiiIdCall = {
    reader.readStartDocument()

    reader.readName(idN)
    val piiId = reader.readObjectId()

    reader.readName(refN)
    val ref: CallRef = ctx.decodeWithChildContext( refCodec, reader )

    reader.readEndDocument()
    KeyPiiIdCall( piiId, ref )
  }

  override def encode(writer: BsonWriter, key: KeyPiiIdCall, ctx: EncoderContext): Unit = {
    writer.writeStartDocument()

    writer.writeName(idN)
    writer.writeObjectId( key.piiId )

    writer.writeName(refN)
    ctx.encodeWithChildContext( refCodec, writer, key.ref )

    writer.writeEndDocument()
  }

  override def getEncoderClass: Class[KeyPiiIdCall] = KEY_PII_ID_CALL
}
