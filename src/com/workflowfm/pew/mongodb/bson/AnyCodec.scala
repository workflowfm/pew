package com.workflowfm.pew.mongodb.bson

import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.codecs.configuration.CodecRegistry

class AnyCodec( registry: CodecRegistry )
  extends Codec[Any] {

  val classN: String = "class"
  val childN: String = "child"

  def codec( clazz: Class[_] ): Codec[Any]
    = registry.get( clazz.asInstanceOf[Class[Any]] )

  override def encode(writer: BsonWriter, value: Any, ctx: EncoderContext): Unit = {

    writer.writeStartDocument()

    // Jev, `getCanonicalName` produces incorrect results for packaged or inner classes.
    // We need the `fully qualified` names for `Class.forName`, eg, "some.package.Object$Innerclass"
    val className: String = value.getClass.getName // .getCanonicalName)
    Class.forName( className ) // throw a ClassNotFound error if we won't be able to decode this.
    writer.writeString( classN, className )

    ctx.encodeWithChildContext( codec( value.getClass ), writer, value )

    writer.writeEndDocument()
  }

  override def decode(reader: BsonReader, ctx: DecoderContext): Any = {

    reader.readStartDocument()

    val anyClass: Class[_] = Class.forName( reader.readString( classN ) )

    val anyValue: Any = ctx.decodeWithChildContext[Any]( codec( anyClass ), reader )

    reader.readEndDocument()

    anyValue
  }

  override def getEncoderClass: Class[Any] = classOf[Any]
}
