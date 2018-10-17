package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.messages

import com.workflowfm.pew._
import com.workflowfm.pew.stateless.StatelessMessages.PiiLog
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.PewCodecs
import org.bson._
import org.bson.codecs._
import org.bson.types.ObjectId

class PiiLogCodec( eventCodec: Codec[PiEvent[ObjectId]] )
  extends Codec[PiiLog] {

  import PewCodecs._

  override def decode(reader: BsonReader, ctx: DecoderContext): PiiLog
    = PiiLog( ctx.decodeWithChildContext( eventCodec, reader ) )

  override def encode(writer: BsonWriter, value: PiiLog, ctx: EncoderContext): Unit
    = ctx.encodeWithChildContext( eventCodec, writer, value.event )

  override def getEncoderClass: Class[PiiLog] = PIILOG

}