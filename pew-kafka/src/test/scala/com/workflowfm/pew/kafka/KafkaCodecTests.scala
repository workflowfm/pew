package com.workflowfm.pew.kafka

import scala.reflect.ClassTag
import scala.runtime.BoxedUnit

import org.bson.{ BsonReader, BsonWriter }
import org.bson.codecs.{ Codec, DecoderContext, EncoderContext }
import org.bson.codecs.configuration.CodecRegistry
import org.bson.types.ObjectId
import org.junit.runner.RunWith
import org.scalatest
import org.scalatest.junit.JUnitRunner

import com.workflowfm.pew._
import com.workflowfm.pew.mongodb.bson.AnyCodec
import com.workflowfm.pew.mongodb.bson.auto.ClassCodec
import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.kafka.settings.KafkaExecutorSettings.{
  AnyKey,
  KeyPiiId,
  KeyPiiIdCall
}
import com.workflowfm.pew.kafka.settings.bson.{
  CodecWrapper,
  KafkaCodecRegistry
}

@RunWith(classOf[JUnitRunner])
class KafkaCodecTests extends PewTestSuite with KafkaExampleTypes {

  case class TestObject(arg1: String, arg2: Int)

  class TestObjectCodec extends ClassCodec[TestObject] {

    override def decodeBody(reader: BsonReader, ctx: DecoderContext): TestObject = {
      reader.readName("1")
      val arg1 = reader.readString()

      reader.readName("2")
      val arg2 = reader.readInt32()

      TestObject(arg1, arg2)
    }

    override def encodeBody(writer: BsonWriter, value: TestObject, ctx: EncoderContext): Unit = {
      writer.writeName("1")
      writer.writeString(value.arg1)
      writer.writeName("2")
      writer.writeInt32(value.arg2)
    }
  }

  object ExtendedCodecRegistry extends KafkaCodecRegistry(completeProcess.store) {
    val testCodec: Codec[TestObject] = new TestObjectCodec with AutoCodec
  }

  def testCodec[T](
      tOriginal: T
  )(implicit ct: ClassTag[T], testRegistry: CodecRegistry): scalatest.Assertion = {

    testRegistry.get[T](ct.runtimeClass.asInstanceOf[Class[T]]) shouldNot be(null)

    val wrapper = CodecWrapper[T](ct, testRegistry)
    val bytes = wrapper.serialize("FakeTopic", tOriginal)
    val tReserialized = wrapper.deserialize("FakeTopic", bytes)

    tReserialized shouldEqual tOriginal
  }

  implicit lazy val registry: CodecRegistry = ExtendedCodecRegistry

  //---------------------//
  //--- Sanity Checks ---//
  //---------------------//

  it should "expose itself via the AnyCodec" in {
    val anyc: AnyCodec = new AnyCodec(ExtendedCodecRegistry)

    val codecs: Seq[Codec[_]] = ExtendedCodecRegistry.registeredCodecs.values
      .map(c => anyc.codec(c.getEncoderClass))
      .toSeq

    codecs should not contain null
  }

  it should "correctly expose additional Codecs" in {
    ExtendedCodecRegistry.registeredCodecs.values should contain(ExtendedCodecRegistry.testCodec)
    ExtendedCodecRegistry.registeredCodecs.keys should contain(classOf[TestObject])
    ExtendedCodecRegistry.testCodec.asInstanceOf[Codec[TestObject]] shouldNot be(null)
  }

  //----------------------//
  //--- External Types ---//
  //----------------------//

  lazy val easyTestObj: TestObject = TestObject("easy", 1)
  lazy val hardTestObj: TestObject = TestObject("H4rD!!``", -1)
  lazy val emptyTestObj: TestObject = TestObject("", 0)

  it should "correctly (de)serialise additional Codecs" in {
    testCodec(easyTestObj)
    testCodec(hardTestObj)
    testCodec(emptyTestObj)
  }

  it should "expose itself via to AnyCodec to PiObjects" in {
    testCodec(PiItem(TestObject("easy", 1)))
    testCodec(PiItem(TestObject("H4rD!!``", -1)))
    testCodec(PiItem(TestObject("", 0)))
  }

  it should "correctly (de)serialise Tuples" in {
    testCodec[(Int, String)]((1, "Hello, World!"))
    testCodec[(TestObject, TestObject)]((easyTestObj, hardTestObj))
  }

  it should "correctly (de)serialise Options" in {
    type ValueT = Option[String]

    testCodec[ValueT](None)
    testCodec[ValueT](Some("Hello, World!"))
  }

  it should "correctly (de)serialise raw Optons" in {
    testCodec(None)
    testCodec(Some("Hello, World!"))
  }

  it should "correctly (de)serialise Eithers" in {
    type ValueT = Either[String, Int]

    testCodec[ValueT](Left("hi"))
    testCodec[ValueT](Right(1))
  }

  it should "correctly (de)serialise raw Eithers" in {
    testCodec(Left("hi"))
    testCodec(Right(1))
  }

  it should "correctly (de)serialise BoxUnits" in {
    testCodec(BoxedUnit.UNIT)
  }

  //-------------------//
  //--- Kafka Types ---//
  //-------------------//

  it should "correctly (de)serialise KeyPiiIds" in {
    testCodec(KeyPiiId(ObjectId.get))
  }

  it should "correctly (de)serialise KeyPiiIdCall" in {
    testCodec(KeyPiiIdCall(ObjectId.get, CallRef(0)))
    testCodec(KeyPiiIdCall(ObjectId.get, CallRef(1)))
  }

  it should "correctly (de)serialise AnyKeys" in {
    testCodec[AnyKey](KeyPiiId(ObjectId.get))
    testCodec[AnyKey](KeyPiiIdCall(ObjectId.get, CallRef(0)))
    testCodec[AnyKey](KeyPiiIdCall(ObjectId.get, CallRef(1)))
  }

  it should "correctly (de)serialise ReduceRequests" in {
    testCodec(eg1.rrNew)
    testCodec(eg1.rrInProgress)
    testCodec(eg1.rrFinishing2)
    testCodec(eg1.rrFinishing3)
    testCodec(eg1.rrFinishing23)
  }

  it should "correctly (de)serialise SequenceRequests" in {
    testCodec(eg1.srInProgress)
    testCodec(eg1.srFinishing2)
    testCodec(eg1.srFinishing3)
  }

  it should "correctly (de)serialise PiExceptionEvents" in {
    testCodec(NoResultException(eg1.pInProgress).event)
    testCodec(UnknownProcessException(eg1.pInProgress, "ProcessName").event)
    testCodec(AtomicProcessIsCompositeException(eg1.pInProgress, "ProcessName").event)
    testCodec(NoSuchInstanceException(eg1.piiId).event)

    testCodec(RemoteException(eg1.piiId, new TestException).event)
    testCodec(RemoteProcessException(eg1.piiId, 0, new TestException).event)
  }

  it should "correctly (de)serialise SequenceFailures" in {
    testCodec(eg1.sfInProgress)
    testCodec(eg1.sfFinishing21)
    testCodec(eg1.sfFinishing22)
    testCodec(eg1.sfFinishing3)

    testCodec(SequenceFailure(Left(eg1.piiId), Seq(), Seq()))
    testCodec(SequenceFailure(Right(eg1.pInProgress), Seq(), Seq()))
  }

  it should "correctly (de)serialise PiiUpdates" in {
    testCodec(PiiUpdate(eg1.pNew))
    testCodec(PiiUpdate(eg1.pInProgress))
    testCodec(PiiUpdate(eg1.pFinishing))
    testCodec(PiiUpdate(eg1.pCompleted))
  }

  it should "correctly (de)serialise mixed PiiHistory types" in {
    testCodec[PiiHistory](PiiUpdate(eg1.pNew))
    testCodec[PiiHistory](PiiUpdate(eg1.pInProgress))
    testCodec[PiiHistory](PiiUpdate(eg1.pFinishing))
    testCodec[PiiHistory](PiiUpdate(eg1.pCompleted))

    testCodec[PiiHistory](eg1.srInProgress)
    testCodec[PiiHistory](eg1.srFinishing2)
    testCodec[PiiHistory](eg1.srFinishing3)

    testCodec[PiiHistory](eg1.sfInProgress)
    testCodec[PiiHistory](eg1.sfFinishing21)
    testCodec[PiiHistory](eg1.sfFinishing22)
    testCodec[PiiHistory](eg1.sfFinishing3)
  }

  it should "correctly (de)serialise Assignments" in {
    testCodec(eg1.assgnInProgress)
    testCodec(eg1.assgnFinishing2)
    testCodec(eg1.assgnFinishing3)
  }

  it should "correctly (de)serialise PiiLogs" in {
    testCodec(PiiLog(PiEventStart(eg1.pNew)))

    testCodec(PiiLog(PiEventResult(eg1.pCompleted, callRes0._2)))
    testCodec(PiiLog(PiEventResult(eg1.pCompleted, callResHi._2)))

    testCodec(PiiLog(PiEventCall(eg1.piiId, eg1.r1._1.id, eg1.proc1, Seq(eg1.arg1))))
    testCodec(PiiLog(PiEventCall(eg1.piiId, eg1.r2._1.id, eg1.proc2, Seq(eg1.arg2))))
    testCodec(PiiLog(PiEventCall(eg1.piiId, eg1.r3._1.id, eg1.proc3, Seq(eg1.arg3))))

    testCodec(PiiLog(PiEventReturn(eg1.piiId, eg1.r1._1.id, eg1.r1._2)))
    testCodec(PiiLog(PiEventReturn(eg1.piiId, eg1.r2._1.id, eg1.r2._2)))
    testCodec(PiiLog(PiEventReturn(eg1.piiId, eg1.r3._1.id, eg1.r3._2)))

    testCodec(PiiLog(PiFailureNoResult(eg1.pNew)))
    testCodec(PiiLog(PiFailureUnknownProcess(eg1.pInProgress, "SumPr0ssess")))
    testCodec(PiiLog(PiFailureAtomicProcessIsComposite(eg1.pFinishing, "SomeCompositeProc")))
    testCodec(PiiLog(PiFailureNoSuchInstance(eg1.piiId)))
    testCodec(PiiLog(PiFailureExceptions(eg1.piiId, testException)))
    testCodec(PiiLog(PiFailureAtomicProcessException(eg1.piiId, eg1.r1._1.id, testException)))
  }

  it should "correctly (de)serialise mixed AnyMsgs" in {
    testCodec[AnyMsg](eg1.rrNew)
    testCodec[AnyMsg](eg1.rrInProgress)
    testCodec[AnyMsg](eg1.rrFinishing2)
    testCodec[AnyMsg](eg1.rrFinishing3)
    testCodec[AnyMsg](eg1.rrFinishing23)

    testCodec[AnyMsg](PiiUpdate(eg1.pNew))
    testCodec[AnyMsg](PiiUpdate(eg1.pInProgress))
    testCodec[AnyMsg](PiiUpdate(eg1.pFinishing))
    testCodec[AnyMsg](PiiUpdate(eg1.pCompleted))

    testCodec[AnyMsg](eg1.srInProgress)
    testCodec[AnyMsg](eg1.srFinishing2)
    testCodec[AnyMsg](eg1.srFinishing3)

    testCodec[AnyMsg](eg1.sfInProgress)
    testCodec[AnyMsg](eg1.sfFinishing21)
    testCodec[AnyMsg](eg1.sfFinishing22)
    testCodec[AnyMsg](eg1.sfFinishing3)

    testCodec[AnyMsg](eg1.assgnInProgress)
    testCodec[AnyMsg](eg1.assgnFinishing2)
    testCodec[AnyMsg](eg1.assgnFinishing3)

    testCodec[AnyMsg](PiiLog(PiEventStart(eg1.pNew)))
    testCodec[AnyMsg](PiiLog(PiEventResult(eg1.pCompleted, callRes0._2)))
    testCodec[AnyMsg](PiiLog(PiEventResult(eg1.pCompleted, callResHi._2)))
    testCodec[AnyMsg](PiiLog(PiEventCall(eg1.piiId, eg1.r1._1.id, eg1.proc1, Seq(eg1.arg1))))
    testCodec[AnyMsg](PiiLog(PiEventCall(eg1.piiId, eg1.r2._1.id, eg1.proc2, Seq(eg1.arg2))))
    testCodec[AnyMsg](PiiLog(PiEventCall(eg1.piiId, eg1.r3._1.id, eg1.proc3, Seq(eg1.arg3))))
    testCodec[AnyMsg](PiiLog(PiEventReturn(eg1.piiId, eg1.r1._1.id, eg1.r1._2)))
    testCodec[AnyMsg](PiiLog(PiEventReturn(eg1.piiId, eg1.r2._1.id, eg1.r2._2)))
    testCodec[AnyMsg](PiiLog(PiEventReturn(eg1.piiId, eg1.r3._1.id, eg1.r3._2)))
    testCodec[AnyMsg](PiiLog(PiFailureNoResult(eg1.pNew)))
    testCodec[AnyMsg](PiiLog(PiFailureUnknownProcess(eg1.pInProgress, "SumPr0ssess")))
    testCodec[AnyMsg](
      PiiLog(PiFailureAtomicProcessIsComposite(eg1.pFinishing, "SomeCompositeProc"))
    )
    testCodec[AnyMsg](PiiLog(PiFailureNoSuchInstance(eg1.piiId)))
    testCodec[AnyMsg](PiiLog(PiFailureExceptions(eg1.piiId, testException)))
    testCodec[AnyMsg](
      PiiLog(PiFailureAtomicProcessException(eg1.piiId, eg1.r1._1.id, testException))
    )
  }
}
