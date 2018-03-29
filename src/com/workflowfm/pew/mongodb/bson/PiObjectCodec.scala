package com.workflowfm.pew.mongodb.bson

import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import com.workflowfm.pew._
import org.bson.types._
import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.CodecProvider
import org.bson.codecs.configuration.CodecRegistry

class PiObjectCodec(registry: CodecRegistry) extends Codec[PiObject] { 
  def this() = this(DEFAULT_CODEC_REGISTRY)
  
  override def encode(writer: BsonWriter, value: PiObject, encoderContext: EncoderContext): Unit = {
    writer.writeStartDocument()
    writer.writeName("_t")
    value match {
      case PiItem(i) => {
        writer.writeString("PiItem")
        writer.writeName("class")
        writer.writeString(i.getClass().getCanonicalName)
        writer.writeName("i")
        encodeChild(writer,i,encoderContext)
      }
      case Chan(s) => {
    	  writer.writeString("Chan")
        writer.writeName("s")
        writer.writeString(s)
      }
      case PiPair(l,r) => {
        writer.writeString("PiPair")
        writer.writeName("l")
        encoderContext.encodeWithChildContext(this,writer,l)
        writer.writeName("r")
        encoderContext.encodeWithChildContext(this,writer,r)
      }
      case PiOpt(l,r) => {
        writer.writeString("PiOpt")
        writer.writeName("l")
        encoderContext.encodeWithChildContext(this,writer,l)
        writer.writeName("r")
        encoderContext.encodeWithChildContext(this,writer,r)
      }
      case PiLeft(l) => {
        writer.writeString("PiLeft")
        writer.writeName("l")
        encoderContext.encodeWithChildContext(this,writer,l)
      }
      case PiRight(r) => {
        writer.writeString("PiRight")
        writer.writeName("r")
        encoderContext.encodeWithChildContext(this,writer,r)
      }    
   }
   writer.writeEndDocument() }

  def encodeChild[V](writer: BsonWriter, value: V, encoderContext: EncoderContext): Unit = {
    val clazz = value.getClass
    encoderContext.encodeWithChildContext(registry.get(clazz).asInstanceOf[Encoder[V]], writer, value)
  } 
  
  override def getEncoderClass: Class[PiObject] = classOf[PiObject]

  override def decode(reader: BsonReader, decoderContext: DecoderContext): PiObject = {
    reader.readStartDocument()
    reader.readName() // "_t"
    val ret:PiObject = reader.readString() match {
      case "PiItem" => {
        reader.readName("class")
        val clazz = Class.forName(reader.readString())
        reader.readName("i")
        PiItem(decoderContext.decodeWithChildContext(registry.get(clazz), reader))
      }
      case "Chan" => {
        reader.readName("s")
        Chan(reader.readString())
      }
      case "PiPair" => {
        reader.readName("l")
        val l = decoderContext.decodeWithChildContext(this,reader)
        reader.readName("r")
        val r = decoderContext.decodeWithChildContext(this,reader)
        PiPair(l,r)
      }
      case "PiOpt" => {
        reader.readName("l")
        val l = decoderContext.decodeWithChildContext(this,reader)
        reader.readName("r")
        val r = decoderContext.decodeWithChildContext(this,reader)
        PiOpt(l,r)
      }
      case "PiLeft" => {
        reader.readName("l")
        val l = decoderContext.decodeWithChildContext(this,reader)
        PiLeft(l)
      }
      case "PiRight" => {
        reader.readName("r")
        val r = decoderContext.decodeWithChildContext(this,reader)
        PiRight(r)
      }
    }
    reader.readEndDocument()
    ret
  }
}

/**
 * CodecProvider for PiObjectCodec
 * 
 * Created using this as an example:
 * https://github.com/mongodb/mongo-scala-driver/blob/master/bson/src/main/scala/org/mongodb/scala/bson/codecs/DocumentCodecProvider.scala
 */
class PiObjectCodecProvider(bsonTypeClassMap:BsonTypeClassMap) extends CodecProvider {
  val PROVIDEDCLASS:Class[PiObject] =  classOf[PiObject]  
  
  override def get[T](clazz:Class[T], registry:CodecRegistry):Codec[T] = clazz match {
      case PROVIDEDCLASS => new PiObjectCodec(registry).asInstanceOf[Codec[T]]
      case _ => null
    }
}