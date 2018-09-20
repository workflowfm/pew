package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs

import com.workflowfm.pew.stateless.instances.kafka.KafkaTopic._
import org.bson.codecs._
import org.bson.{BsonReader, BsonWriter}

class AnyKeyCodec(
    keyPiiCodec:      Codec[KeyPii],
    keyPiiCallCodec:  Codec[KeyPiiCall],

    keyPidCodec:      Codec[KeyPiiId],
    keyPidCallCodec:  Codec[KeyPiiIdCall]

  ) extends Codec[AnyKey] {

  import PewCodecs._

  override def encode(writer: BsonWriter, value: AnyKey, ctx: EncoderContext): Unit = {
    value match {
      case k: KeyPiiId      => keyPidCodec.encode( writer, k, ctx )
      case k: KeyPiiIdCall  => keyPidCallCodec.encode( writer, k, ctx )
    }
  }

  override def decode(reader: BsonReader, decoderContext: DecoderContext): AnyKey = ???

  override def getEncoderClass: Class[AnyKey] = ANY_KEY
}
