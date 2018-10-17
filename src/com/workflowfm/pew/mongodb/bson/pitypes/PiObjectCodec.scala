package com.workflowfm.pew.mongodb.bson.pitypes

import com.workflowfm.pew._
import com.workflowfm.pew.mongodb.bson._
import com.workflowfm.pew.mongodb.bson.auto.{ClassCodec, SuperclassCodec}
import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY

import scala.collection.mutable.Queue

class PiItemCodec( anyCodec: Codec[Any] )
  extends ClassCodec[PiItem[Any]] {

  val iN: String = "i"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiItem[Any] = {
    reader.readName( iN )
    val i: Any = ctx.decodeWithChildContext( anyCodec, reader )
    PiItem( i )
  }

  override def encodeBody(writer: BsonWriter, value: PiItem[Any], ctx: EncoderContext): Unit = {
    writer.writeName( iN )
    ctx.encodeWithChildContext( anyCodec, writer, value.i )
  }
}

class ChanCodec extends ClassCodec[Chan] {

  val sN: String = "s"

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): Chan
    = Chan( reader.readString( sN ) )

  override def encodeBody(writer: BsonWriter, value: Chan, ctx: EncoderContext): Unit
    = writer.writeString( sN, value.s )
}

class PiPairCodec( objCodec: Codec[PiObject] )
  extends ClassCodec[PiPair] {

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiPair = {
    reader.readName("l")
    val l = ctx.decodeWithChildContext( objCodec, reader )
    reader.readName("r")
    val r = ctx.decodeWithChildContext( objCodec, reader )
    PiPair(l,r)
  }

  override def encodeBody(writer: BsonWriter, value: PiPair, ctx: EncoderContext): Unit = {
    writer.writeName("l")
    ctx.encodeWithChildContext( objCodec, writer, value.l )
    writer.writeName("r")
    ctx.encodeWithChildContext( objCodec, writer, value.r )
  }
}

class PiOptCodec( objCodec: Codec[PiObject] )
  extends ClassCodec[PiOpt] {

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiOpt = {
    reader.readName("l")
    val l = ctx.decodeWithChildContext( objCodec, reader )
    reader.readName("r")
    val r = ctx.decodeWithChildContext( objCodec, reader )
    PiOpt(l,r)
  }

  override def encodeBody(writer: BsonWriter, value: PiOpt, ctx: EncoderContext): Unit = {
    writer.writeName("l")
    ctx.encodeWithChildContext( objCodec, writer, value.l )
    writer.writeName("r")
    ctx.encodeWithChildContext( objCodec, writer, value.r )
  }
}

class PiLeftCodec( objCodec: Codec[PiObject] )
  extends ClassCodec[PiLeft] {

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiLeft = {
    reader.readName("l")
    val l = ctx.decodeWithChildContext( objCodec, reader )
    PiLeft(l)
  }

  override def encodeBody(writer: BsonWriter, value: PiLeft, ctx: EncoderContext): Unit = {
    writer.writeName("l")
    ctx.encodeWithChildContext( objCodec, writer, value.l )
  }
}

class PiRightCodec( objCodec: Codec[PiObject] )
  extends ClassCodec[PiRight] {

  override def decodeBody(reader: BsonReader, ctx: DecoderContext): PiRight = {
    reader.readName("r")
    val r = ctx.decodeWithChildContext( objCodec, reader )
    PiRight(r)
  }

  override def encodeBody(writer: BsonWriter, value: PiRight, ctx: EncoderContext): Unit = {
    writer.writeName("r")
    ctx.encodeWithChildContext( objCodec, writer, value.r )
  }
}

class PiObjectCodec( registry: CodecRegistry = DEFAULT_CODEC_REGISTRY )
  extends SuperclassCodec[PiObject] {

  val anyCodec: Codec[Any] = new AnyCodec( registry )

  updateWith( new PiItemCodec( anyCodec ) )
  updateWith( new ChanCodec() )
  updateWith( new PiPairCodec( this ) )
  updateWith( new PiOptCodec( this ) )
  updateWith( new PiLeftCodec( this ) )
  updateWith( new PiRightCodec( this ) )
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

  import BsonUtil._

  override def encode(writer: BsonWriter, value: PiFuture, encoderContext: EncoderContext): Unit = { 
	  writer.writeStartDocument()
//	  writer.writeName("_t")
//	  writer.writeString("PiFuture")
    writer.writeName("fun")
    writer.writeString(value.fun)
    writer.writeName("oc")
	  encoderContext.encodeWithChildContext(chanCodec,writer,value.outChan)

    writeArray( writer, "args", value.args ) {
      encoderContext.encodeWithChildContext( resourceCodec, writer, _ )
    }

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

    val args: List[PiResource]
      = readArray( reader, "args" ) { () =>
        decoderContext.decodeWithChildContext( resourceCodec, reader )
      }

	  reader.readEndDocument()
	  PiFuture( f, oc, args )
  }
}

