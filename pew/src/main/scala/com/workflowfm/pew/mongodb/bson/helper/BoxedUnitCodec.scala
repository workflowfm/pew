package com.workflowfm.pew.mongodb.bson.helper

import scala.runtime.BoxedUnit

import org.bson.{ BsonReader, BsonWriter }
import org.bson.codecs.{ DecoderContext, EncoderContext }

import com.workflowfm.pew.mongodb.bson.auto.ClassCodec

class BoxedUnitCodec extends ClassCodec[BoxedUnit] {
  override def decodeBody(reader: BsonReader, ctx: DecoderContext): BoxedUnit = BoxedUnit.UNIT
  override def encodeBody(writer: BsonWriter, value: BoxedUnit, ctx: EncoderContext): Unit = {}
}
