package com.workflowfm.pew.mongodb.bson.helper

import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import org.bson.codecs.{ DecoderContext, EncoderContext }
import org.bson.{ BsonReader, BsonWriter }

import scala.runtime.BoxedUnit

class BoxedUnitCodec extends ClassCodec[BoxedUnit] {
  override def decodeBody(reader: BsonReader, ctx: DecoderContext): BoxedUnit = BoxedUnit.UNIT
  override def encodeBody(writer: BsonWriter, value: BoxedUnit, ctx: EncoderContext): Unit = {}
}
