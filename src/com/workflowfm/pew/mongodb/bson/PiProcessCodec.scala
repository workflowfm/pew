package com.workflowfm.pew.mongodb.bson

import com.workflowfm.pew.{PiProcess, PiProcessStore}
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.{BsonReader, BsonWriter}

class PiProcessCodec( store: PiProcessStore )
  extends Codec[PiProcess] {

  override def encode(writer: BsonWriter, value: PiProcess, ctx: EncoderContext): Unit
    = writer.writeString( value.iname )

  override def decode(reader: BsonReader, ctx: DecoderContext): PiProcess = {
    val name: String = reader.readString()
    store.get( name ) match {
      case None => throw new CodecConfigurationException(s"Unknown PiProcess: $name")
      case Some( proc ) => proc
    }
  }

  override def getEncoderClass: Class[PiProcess]
    = classOf[PiProcess]
}
