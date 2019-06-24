package com.workflowfm.pew.mongodb.bson.auto

import org.bson.{BsonReader, BsonReaderMark, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

import scala.collection.mutable
import scala.reflect.ClassTag

abstract class TypedCodec[T](implicit ct: ClassTag[T]) extends Codec[T] {

  val clazz: Class[T]  = ct.runtimeClass.asInstanceOf[Class[T]]
  val _typeVal: String = clazz.getName

  final override def getEncoderClass: Class[T] = clazz

}

object TypedCodec {
  val _typeN: String = "__typeID"
}

abstract class ClassCodec[T](implicit ct: ClassTag[T]) extends TypedCodec[T]()(ct) {

  import TypedCodec._

  def decodeBody(reader: BsonReader, ctx: DecoderContext): T
  def encodeBody(writer: BsonWriter, value: T, ctx: EncoderContext): Unit

  final override def decode(reader: BsonReader, ctx: DecoderContext): T = {
    reader.readStartDocument()
    reader.readString(_typeN)
    val t: T = decodeBody(reader, ctx)
    reader.readEndDocument()
    t
  }

  final override def encode(writer: BsonWriter, value: T, ctx: EncoderContext): Unit = {
    writer.writeStartDocument()
    writer.writeString(_typeN, _typeVal)
    encodeBody(writer, value, ctx)
    writer.writeEndDocument()
  }
}

/** A codec consisting of a collection of ClassCodecs of possible subtypes.
  * - Examines the class name when decoding to defer to the correct ClassCodec for de-serialisation
  * - Uses the value class to defer to the correct ClassCodec for serialisation.
  */
class SuperclassCodec[T](implicit ct: ClassTag[T]) extends TypedCodec[T]()(ct) {

  import TypedCodec._

  private val byClass: mutable.Map[Class[_], ClassCodec[_]] = mutable.Map()
  private val byClassId: mutable.Map[String, ClassCodec[_]] = mutable.Map()

  def updateWith(codec: ClassCodec[_]): Unit = if (clazz isAssignableFrom codec.clazz) {
    println(s"SuperclassCodec(${_typeVal}): Registered subclass '${codec._typeVal}'.")
    byClass.update(codec.clazz, codec)
    byClassId.update(codec._typeVal, codec)
  }

  def knownChildren: Iterable[ClassCodec[_]] = byClass.values

  override def decode(reader: BsonReader, ctx: DecoderContext): T = {
    val mark: BsonReaderMark = reader.getMark

    reader.readStartDocument()
    val typeId: String = reader.readString(_typeN)
    mark.reset()

    ctx.decodeWithChildContext(byClassId(typeId), reader).asInstanceOf[T]
  }

  override def encode(writer: BsonWriter, value: T, ctx: EncoderContext): Unit =
    byClass(value.getClass)
      .asInstanceOf[ClassCodec[T]]
      .encode(writer, value, ctx)
}
