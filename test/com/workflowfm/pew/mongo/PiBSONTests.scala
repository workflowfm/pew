package com.workflowfm.pew.mongo

import org.mongodb.scala.bson.collection.immutable.Document
import org.bson.codecs.configuration.CodecRegistries.{fromRegistries, fromProviders, fromCodecs}
import org.mongodb.scala.bson.codecs.Macros
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import com.workflowfm.pew._
import org.bson.types._
import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.CodecProvider
import org.bson.codecs.configuration.CodecRegistry
import org.bson.codecs.configuration.CodecRegistries


import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.bson.io._

import java.nio.ByteBuffer
import java.util

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.reflect.ClassTag

@RunWith(classOf[JUnitRunner])
class PiBSONTests extends FlatSpec with Matchers with PiBSONTestHelper {
	
  //val p = new PiObjectCodecProvider
  
  val chanCodecProvider = Macros.createCodecProvider[Chan]()
  val pairCodecProvider = Macros.createCodecProvider[PiPair]()
  val optCodecProvider = Macros.createCodecProvider[PiOpt]()
  val leftCodecProvider = Macros.createCodecProvider[PiLeft]()
  val rightCodecProvider = Macros.createCodecProvider[PiRight]()
  val reg = fromRegistries( 
	      fromProviders( 
	        chanCodecProvider, 
	        pairCodecProvider, 
	        optCodecProvider,
	        leftCodecProvider,
	        rightCodecProvider),
	      fromCodecs(new PiItemCodec),
	      DEFAULT_CODEC_REGISTRY) 
  
  //val r = fromRegistries(fromProviders(p), DEFAULT_CODEC_REGISTRY
  
	"codec" should "encode/decode PiItems" in {
//	  val obj = PiItem("Oh!")
//	  
	  val codec = new PiItemCodec
//
//	  val writer: BsonBinaryWriter = new BsonBinaryWriter(new BasicOutputBuffer())
//	  codec.encode(writer, obj, EncoderContext.builder().build())
//	  
//	  val buffer: BasicOutputBuffer = writer.getBsonOutput().asInstanceOf[BasicOutputBuffer];
//	  val reader: BsonBinaryReader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray))))
//		val dec = codec.decode(reader, DecoderContext.builder().build())
//		
//		obj should be (dec)
	  
	  roundTrip(PiItem("Oh!"), """{piitem: "Oh!"}""", codec)
	  roundTrip(PiItem(9), """{piitem: 9}""", codec)
  } 
  
  
// This doesn't work because the decoder doesn't know how to deal with the (sub-)Document and that it should be mapped to a PiItem.
// You either (a) manually decode the Document to a PiItem or (b) use a bsonTypeCodecMap to map Documents to PiItems 
  
//	"codec" should "encode/decode a Document with a PiItem" in {
//	  val obj = PiItem("Oh!")
//	  
//	  val doc = new Document().append("x", obj)
//	  
//	   System.err.println(">>> " + doc + " | " + doc.get("x") + ":" + doc.get("x").getClass)
//	  
//	  val codec = new PiItemCodec
//	  val reg = fromRegistries(fromCodecs(codec), DEFAULT_CODEC_REGISTRY) 
//	  val docc = new DocumentCodec(reg)
//
//	  val writer: BsonBinaryWriter = new BsonBinaryWriter(new BasicOutputBuffer())
//	  docc.encode(writer, doc, EncoderContext.builder().build())
//
//	  val buffer: BasicOutputBuffer = writer.getBsonOutput().asInstanceOf[BasicOutputBuffer];
//	  val reader: BsonBinaryReader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray))))
//	  val dec = docc.decode(reader, DecoderContext.builder().build())
//		
//	  System.err.println(">>>! " + dec + " | " + dec.get("x") + ":" + dec.get("x").getClass)
//		
//		doc should be (dec)
//  }  
	   
  
//  "codec" should "decode a custom sealed trait somehow" in {
//    sealed class Patata
//    case class Ntomata(n:String) extends Patata
//    case class Mpou(mxs:Int) extends Patata
//    
//    sealed class XTree
//    case class XBranch(b1: XTree, b2: XTree, value: Int) extends XTree
//    case class XLeaf(value: Int) extends XTree
//
//    //val codecRegistry = fromRegistries( fromProviders(classOf[Tree]), DEFAULT_CODEC_REGISTRY )
//    
//    val p = Macros.createCodecProvider[XTree]()
//    val p2 = Macros.createCodecProvider[Patata]()
//    //val p3 = Macros.createCodecProvider[PiObject]()
//    val reg = fromRegistries(fromProviders(p),DEFAULT_CODEC_REGISTRY) 
//    
//    roundTrip(XLeaf(5), """{value: 5}""", p)
//  }
  
  it should "encode/decode PiObjects" in {
	  val codec = new PiObjectCodec

	  roundTrip(PiItem("Oh!"), """{"_t": "PiItem", "class":"java.lang.String", "i":"Oh!"}""", codec)
	  roundTrip(Chan("Oh!"), """{"_t": "Chan", "s":"Oh!"}""", codec)
	  roundTrip(PiPair(Chan("L"),PiItem("R")), """{"_t": "PiPair", "l":{"_t" : "Chan", "s":"L"}, "r":{"_t" : "PiItem", "class":"java.lang.String", "i":"R"}}""", codec)
	  
  }
  	
  	
//  "ha" should "tell the truth" in {
//	  val obj = PiPair(PiItem("Oh!"),PiItem("Hi!"))
//
//			  val writer: BsonBinaryWriter = new BsonBinaryWriter(new BasicOutputBuffer())
//			  reg.get(classOf[PiPair]).encode(writer, obj, EncoderContext.builder().build())
//			  System.err.println(">>>" + writer.getBsonOutput().asInstanceOf[BasicOutputBuffer].toByteArray())
//
//			  val buffer: BasicOutputBuffer = writer.getBsonOutput().asInstanceOf[BasicOutputBuffer];
//			  val reader: BsonBinaryReader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray))))
//
//					  val dec = reg.get(classOf[PiPair]).decode(reader, DecoderContext.builder().build())
//					  System.err.println(">>>! " + dec)
//					  1 should be (1)
//  }
}

trait PiBSONTestHelper {
  def roundTrip[T](value: T, expected: String, provider: CodecProvider, providers: CodecProvider*)(implicit ct: ClassTag[T]): Unit = {
    val codecProviders: util.List[CodecProvider] = (provider +: providers).asJava
    val registry = CodecRegistries.fromRegistries(CodecRegistries.fromProviders(codecProviders), DEFAULT_CODEC_REGISTRY)
    val codec = registry.get(ct.runtimeClass).asInstanceOf[Codec[T]]
    roundTripCodec(value, Document(expected), codec)
  }
  
  def roundTrip[T](value: T, expected: String, codec: Codec[T]): Unit = {
    roundTripCodec(value, Document(expected), codec)
  }

  def roundTripCodec[T](value: T, expected: Document, codec: Codec[T]): Unit = {
    val encoded = encode(codec, value)
    val actual = decode(documentCodec, encoded)
    System.out.println(s"Encoded document: (${actual.toJson()}) must equal: (${expected.toJson()})")
    assert(expected == actual, s"Encoded document: (${actual.toJson()}) did not equal: (${expected.toJson()})")

    val roundTripped = decode(codec, encode(codec, value))
    System.out.println(s"Round Tripped case class: ($roundTripped) must equal the original: ($value)")
    assert(roundTripped == value, s"Round Tripped case class: ($roundTripped) did not equal the original: ($value)")
  }

  def encode[T](codec: Codec[T], value: T): OutputBuffer = {
    val buffer = new BasicOutputBuffer()
    val writer = new BsonBinaryWriter(buffer)
    codec.encode(writer, value, EncoderContext.builder.build)
    buffer
  }

  def decode[T](codec: Codec[T], buffer: OutputBuffer): T = {
    val reader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray))))
    codec.decode(reader, DecoderContext.builder().build())
  }

  def decode[T](value: T, json: String, codec: Codec[T]): Unit = {
    val roundTripped = decode(codec, encode(documentCodec, Document(json)))
    assert(roundTripped == value, s"Round Tripped case class: ($roundTripped) did not equal the original: ($value)")
  }
  
  val documentCodec: Codec[Document] = DEFAULT_CODEC_REGISTRY.get(classOf[Document])
}