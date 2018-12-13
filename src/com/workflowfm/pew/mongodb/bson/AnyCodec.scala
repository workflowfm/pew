package com.workflowfm.pew.mongodb.bson

import com.workflowfm.pew.util.ClassLoaderUtil
import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}
import org.bson.codecs.configuration.CodecRegistry

/** AnyCodec: Capable of encoding/decoding values of `Any` type by fetching the
  * correct type from a `CodecRegistry`.
  *
  * NOTE: Requested types must be available within the Regsitry specified.
  *
  * @param registry Registry to use to find `Codec`s.
  */
class AnyCodec( val registry: CodecRegistry )
  extends Codec[Any] {

  val classN: String = "class"
  val childN: String = "child"

  def classLoader: ClassLoader = null

  def codec( clazz: Class[_] ): Codec[Any]
    = registry.get( clazz.asInstanceOf[Class[Any]] )

  // Jev, unset the `ClassLoader` to ensure the default is used.
  def classForName( name: String ): Class[_]
    = ClassLoaderUtil.withClassLoader( classLoader ) { Class.forName(name) }

  override def encode(writer: BsonWriter, value: Any, ctx: EncoderContext): Unit = {

    writer.writeStartDocument()

    // Jev, `getCanonicalName` produces incorrect results for packaged or inner classes.
    // We need the `fully qualified` names for `Class.forName`, eg, "some.package.Object$Innerclass"
    val className: String = value.getClass.getName // .getCanonicalName)
    classForName( className ) // throw a ClassNotFound error if we won't be able to decode this.
    writer.writeString(classN, className)

    writer.writeName( childN )
    ctx.encodeWithChildContext( codec( value.getClass ), writer, value )

    writer.writeEndDocument()
  }

  override def decode(reader: BsonReader, ctx: DecoderContext): Any = {

    reader.readStartDocument()

    val anyClass: Class[_] = classForName( reader.readString( classN ) )

    reader.readName( childN )
    val anyValue: Any = ctx.decodeWithChildContext[Any]( codec( anyClass ), reader )

    reader.readEndDocument()

    anyValue
  }

  override def getEncoderClass: Class[Any] = classOf[Any]
}
