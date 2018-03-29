package com.workflowfm.pew.mongodb.bson

import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.bson.codecs.Macros
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import com.workflowfm.pew._

import org.bson.types._
import org.bson._
import org.bson.codecs._
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry, CodecRegistries}
import org.bson.codecs.configuration.CodecRegistries.{fromRegistries, fromProviders, fromCodecs}

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
  
//	"codec" should "encode/decode PiItems" in {
//    val codec = new PiItemCodec
//
//    //	  val obj = PiItem("Oh!")
////	    
////
////	  val writer: BsonBinaryWriter = new BsonBinaryWriter(new BasicOutputBuffer())
////	  codec.encode(writer, obj, EncoderContext.builder().build())
////	  
////	  val buffer: BasicOutputBuffer = writer.getBsonOutput().asInstanceOf[BasicOutputBuffer];
////	  val reader: BsonBinaryReader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray))))
////		val dec = codec.decode(reader, DecoderContext.builder().build())
////		
////		obj should be (dec)
//	  
//	  roundTrip(PiItem("Oh!"), """{piitem: "Oh!"}""", codec)
//	  roundTrip(PiItem(9), """{piitem: 9}""", codec)
//  } 
  
  
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
  
  "PiObjectCodec" should "encode/decode PiObjects" in {
	  val codec = new PiObjectCodec

	  roundTrip(PiItem("Oh!"), """{"_t": "PiItem", "class":"java.lang.String", "i":"Oh!"}""", codec)
	  roundTrip(Chan("Oh!"), """{"_t": "Chan", "s":"Oh!"}""", codec)
	  roundTrip(PiPair(Chan("L"),PiItem("R")), """{"_t": "PiPair", "l":{"_t" : "Chan", "s":"L"}, "r":{"_t" : "PiItem", "class":"java.lang.String", "i":"R"}}""", codec)
	  roundTrip(PiOpt(Chan("L"),PiPair(Chan("RL"),Chan("RR"))), """{"_t": "PiOpt", "l":{"_t" : "Chan", "s":"L"}, "r":{"_t" : "PiPair", "l":{"_t" : "Chan", "s":"RL"}, "r":{"_t" : "Chan", "s":"RR"}}}""", codec)
  }
  
  "TermCodec" should "encode/decode Terms" in {
	  val objcodec = new PiObjectCodec
	  val codec = new TermCodec(fromCodecs(objcodec))

	  roundTrip(ParInI("XC","LC","RC",PiCall<("P1","LC","RC","Z")), codec)
	  val ski = PiCut("z14","x12","oSelectSki_lB_PriceUSD_Plus_Exception_rB_",WithIn("x12","cUSD2NOK_PriceUSD_1","c12",LeftOut("y12","oUSD2NOK_PriceNOK_",PiCall<("USD2NOK","cUSD2NOK_PriceUSD_1","oUSD2NOK_PriceNOK_")),RightOut("y12","d12",PiId("c12","d12","m13"))),PiCut("z9","z8","oSelectModel_lB_Brand_x_Model_rB_",ParInI("z8","cSelectSki_Brand_2","cSelectSki_Model_3",PiCut("z4","cSelectSki_LengthInch_1","oCM2Inch_LengthInch_",PiCall<("SelectSki","cSelectSki_LengthInch_1","cSelectSki_Brand_2","cSelectSki_Model_3","oSelectSki_lB_PriceUSD_Plus_Exception_rB_"),PiCut("z2","cCM2Inch_LengthCM_1","oSelectLength_LengthCM_",PiCall<("CM2Inch","cCM2Inch_LengthCM_1","oCM2Inch_LengthInch_"),PiCall<("SelectLength","cSelectLength_HeightCM_1","cSelectLength_WeightKG_2","oSelectLength_LengthCM_")))),PiCall<("SelectModel","cSelectModel_PriceLimit_1","cSelectModel_SkillLevel_2","oSelectModel_lB_Brand_x_Model_rB_")))
	  roundTrip(ski, codec)
  }
  
  "ChanMapCodec" should "encode/decode ChanMaps" in {
	  val reg = fromRegistries(fromProviders(new PiCodecProvider()),DEFAULT_CODEC_REGISTRY)
	  val codec = reg.get(classOf[ChanMap])

	  roundTrip(ChanMap(), codec)
	  roundTrip(ChanMap((Chan("X"),Chan("Y"))), codec)
	  roundTrip(ChanMap((Chan("X"),PiItem("Oh!")),(Chan("Y"),PiPair(Chan("L"),PiItem("R"))),(Chan("2"),PiOpt(Chan("L"),PiPair(Chan("RL"),Chan("RR"))))), codec)
  }
  
  "PiStateCodec" should "encode/decode PiStates" in {
	  val proc1 = DummyProcess("PROC", Seq("C","R"), "R", Seq((Chan("INPUT"),"C")))
	  val procs = PiProcess.mapOf(proc1)
    
    val reg = fromRegistries(fromProviders(new PiCodecProvider(procs)),DEFAULT_CODEC_REGISTRY)
	  val codec = reg.get(classOf[PiState])
	  //xcodec should be an[PiStateCodec]
	  
	  roundTrip(PiState(In("A","C",PiCall<("PROC","C","R")),Out("A",Chan("X"))) withProc proc1 withCalls proc1.getFuture(Chan("X"),Chan("R")) withSub ("INPUT",PiItem("OHHAI!")), codec)
	  roundTrip(PiState(WithIn("X","L","R",Devour("L","AAA"),WithIn("R","LL","RR",Devour("LL","BBB"),Devour("RR","CCC"))),LeftOut("X","A",Out("A",PiItem("aaa")))) withProc proc1 withThread (0,"PROC","R",Seq(PiResource(PiItem("OHHAI!"),Chan("X")))) incTCtr() withFCtr(5), codec)
	  roundTrip(PiState() withProc proc1 withSub ("RESULT",PiItem("ResultString")) incTCtr() , codec)
  }

  "PiInstanceCodec" should "encode/decode PiInstances" in {
	  val proc1 = DummyProcess("PROC", Seq("C","R"), "R", Seq((Chan("INPUT"),"C")))
	  val procs = PiProcess.mapOf(proc1)
    
    val reg = fromRegistries(fromProviders(new PiCodecProvider(procs)),DEFAULT_CODEC_REGISTRY)
	  val codec = reg.get(classOf[PiInstance[ObjectId]])
	  //xcodec should be an[PiStateCodec]
	  
	  roundTrip(PiInstance(new ObjectId,Seq(), proc1, PiState(In("A","C",PiCall<("PROC","C","R")),Out("A",Chan("X"))) withProc proc1 withCalls proc1.getFuture(Chan("X"),Chan("R")) withSub ("INPUT",PiItem("OHHAI!"))), codec)
	  roundTrip(PiInstance(new ObjectId,Seq(1), proc1, PiState(WithIn("X","L","R",Devour("L","AAA"),WithIn("R","LL","RR",Devour("LL","BBB"),Devour("RR","CCC"))),LeftOut("X","A",Out("A",PiItem("aaa")))) withProc proc1 withThread (0,"PROC","R",Seq(PiResource(PiItem("OHHAI!"),Chan("X")))) incTCtr() withFCtr(5)), codec)
	  roundTrip(PiInstance(new ObjectId,Seq(1,2,3), proc1, PiState() withProc proc1 withSub ("RESULT",PiItem("ResultString")) incTCtr()) , codec)
  }
}

trait PiBSONTestHelper {
  def roundTrip[T](value: T, expected: String, provider: CodecProvider, providers: CodecProvider*)(implicit ct: ClassTag[T]): Unit = {
    val codecProviders: util.List[CodecProvider] = (provider +: providers).asJava
    val registry = CodecRegistries.fromRegistries(CodecRegistries.fromProviders(codecProviders), DEFAULT_CODEC_REGISTRY)
    val codec = registry.get(ct.runtimeClass).asInstanceOf[Codec[T]]
    roundTripCodec(value, Document(expected), codec)
  }

  def roundTrip[T](value: T, codec: Codec[T]): Unit = {
    roundTripCodec(value, codec)
  }
  
  def roundTrip[T](value: T, expected: String, codec: Codec[T]): Unit = {
    roundTripCodec(value, Document(expected), codec)
  }

  def roundTripCodec[T](value: T, codec: Codec[T]): Unit = {
    val encoded = encode(codec, value)
    val actual = decode(documentCodec, encoded)
    System.out.println(s"Encoded document: ${actual.toJson()}")
    val roundTripped = decode(codec, encode(codec, value))
    System.out.println(s"Round Tripped case class: ($roundTripped) must equal the original: ($value)")
    assert(roundTripped == value, s"Round Tripped case class: ($roundTripped) did not equal the original: ($value)")
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