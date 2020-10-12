package com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.messages

import org.bson._
import org.bson.codecs._
import org.bson.types.ObjectId

import com.workflowfm.pew._
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import com.workflowfm.pew.stateless.StatelessMessages.PiiLog

class PiiLogCodec(eventCodec: Codec[PiEvent[ObjectId]]) extends ClassCodec[PiiLog] {

  val eventN: String = "event"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiiLog = {
    reader.readName(eventN)
    val event: PiEvent[ObjectId] = ctx.decodeWithChildContext(eventCodec, reader)

    PiiLog(event)
  }

  override def encodeBody(writer: BsonWriter, value: PiiLog, ctx: EncoderContext): Unit = {
    writer.writeName(eventN)
    ctx.encodeWithChildContext(eventCodec, writer, value.event)
  }

}
