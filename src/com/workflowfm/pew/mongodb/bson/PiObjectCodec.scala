package com.workflowfm.pew.mongodb.bson

import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import com.workflowfm.pew._
import org.bson.types._
import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.CodecProvider
import org.bson.codecs.configuration.CodecRegistry
import scala.collection.mutable.Queue

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
    reader.readName("_t")
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


class ChanCodec(registry: CodecRegistry) extends Codec[Chan] { 
  def this() = this(DEFAULT_CODEC_REGISTRY)
  
  val objCodec = registry.get(classOf[PiObject])
  
  override def encode(writer: BsonWriter, value: Chan, encoderContext: EncoderContext): Unit = 
    objCodec.encode(writer,value,encoderContext)

  override def getEncoderClass: Class[Chan] = classOf[Chan]

  override def decode(reader: BsonReader, decoderContext: DecoderContext): Chan = 
    objCodec.decode(reader,decoderContext).asInstanceOf[Chan] // TODO type check with exception?
}


class ChanMapCodec(registry:CodecRegistry) extends Codec[ChanMap] { 
  val chanCodec:Codec[Chan] = registry.get(classOf[Chan])
  val piobjCodec:Codec[PiObject] = registry.get(classOf[PiObject])
  
  override def encode(writer: BsonWriter, value: ChanMap, encoderContext: EncoderContext): Unit = { 
	  writer.writeStartDocument()
	  writer.writeName("ChanMap")
    writer.writeStartArray()
	  for ((k,v) <- value.map) {
	    writer.writeStartDocument()
	    writer.writeName("k")
	    encoderContext.encodeWithChildContext(chanCodec,writer,k)
	    writer.writeName("v")
	    encoderContext.encodeWithChildContext(piobjCodec,writer,v)
	    writer.writeEndDocument()
	  }
	  writer.writeEndArray()
	  writer.writeEndDocument()
  }
 
  override def getEncoderClass: Class[ChanMap] = classOf[ChanMap]

  override def decode(reader: BsonReader, decoderContext: DecoderContext): ChanMap = {
	  reader.readStartDocument()
	  reader.readName("ChanMap")
    reader.readStartArray()
	  var args:Queue[(Chan,PiObject)] = Queue()
	  while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
	    reader.readStartDocument()
	    reader.readName("k")
		  val k = decoderContext.decodeWithChildContext(chanCodec,reader)
		  reader.readName("v")
		  val v = decoderContext.decodeWithChildContext(piobjCodec,reader)
		  reader.readEndDocument()
		  args+= ((k,v))  
	  }
	  reader.readEndArray()
	  reader.readEndDocument()
	  ChanMap(args:_*)
  }
}

class PiResourceCodec(registry:CodecRegistry) extends Codec[PiResource] { 
  val chanCodec:Codec[Chan] = registry.get(classOf[Chan])
  val piobjCodec:Codec[PiObject] = registry.get(classOf[PiObject])
  
  override def encode(writer: BsonWriter, value: PiResource, encoderContext: EncoderContext): Unit = { 
	  writer.writeStartDocument()
//	  writer.writeName("_t")
//	  writer.writeString("PiResource")
    writer.writeName("obj")
	  encoderContext.encodeWithChildContext(piobjCodec,writer,value.obj)
	  writer.writeName("c")
	  encoderContext.encodeWithChildContext(chanCodec,writer,value.c)
	  writer.writeEndDocument()
  }
 
  override def getEncoderClass: Class[PiResource] = classOf[PiResource]

  override def decode(reader: BsonReader, decoderContext: DecoderContext): PiResource = {
	  reader.readStartDocument()
//	  reader.readName("_t")
//	  reader.readString("PiResource")
    reader.readName("obj")
	  val obj = decoderContext.decodeWithChildContext(piobjCodec,reader)
	  reader.readName("c")
	  val c = decoderContext.decodeWithChildContext(chanCodec,reader)
	  reader.readEndDocument()
	  PiResource(obj,c)
  }
}

class PiFutureCodec(registry:CodecRegistry) extends Codec[PiFuture] { 
  val chanCodec:Codec[Chan] = registry.get(classOf[Chan])
  val resourceCodec:Codec[PiResource] = registry.get(classOf[PiResource])
  
  override def encode(writer: BsonWriter, value: PiFuture, encoderContext: EncoderContext): Unit = { 
	  writer.writeStartDocument()
//	  writer.writeName("_t")
//	  writer.writeString("PiFuture")
    writer.writeName("fun")
    writer.writeString(value.fun)
    writer.writeName("oc")
	  encoderContext.encodeWithChildContext(chanCodec,writer,value.outChan)
	  writer.writeName("args")
	  writer.writeStartArray()
	  for (arg <- value.args) encoderContext.encodeWithChildContext(resourceCodec,writer,arg)
	  writer.writeEndArray()
	  writer.writeEndDocument()
  }
 
  override def getEncoderClass: Class[PiFuture] = classOf[PiFuture]

  override def decode(reader: BsonReader, decoderContext: DecoderContext): PiFuture = {
	  reader.readStartDocument()
//	  reader.readName("_t")
//	  reader.readString("PiFuture")
	  reader.readName("fun")
	  val f = reader.readString()
	  reader.readName("oc")
	  val oc = decoderContext.decodeWithChildContext(chanCodec,reader)
    reader.readName("args")
    reader.readStartArray()
    var args:Queue[PiResource] = Queue()
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
    	val r = decoderContext.decodeWithChildContext(resourceCodec,reader)
    	args+=r
    }
    reader.readEndArray()
	  reader.readEndDocument()
	  PiFuture(f,oc,args.toSeq)
  }
}

