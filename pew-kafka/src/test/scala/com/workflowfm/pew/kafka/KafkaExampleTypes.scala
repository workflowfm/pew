package com.workflowfm.pew.kafka

import org.bson.types.ObjectId

import com.workflowfm.pew._
import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.StatelessMessages._

//noinspection TypeAnnotation
trait KafkaExampleTypes extends KafkaTests {

  val p1: PiInstance[ObjectId] = PiInstance(ObjectId.get, pbi, PiObject(1))
  val p2: PiInstance[ObjectId] = PiInstance(ObjectId.get, pbi, PiObject(1))

  class TestException extends Exception("test argh!")

  val testException = new TestException

  val callRes0: (CallRef, PiObject) = (CallRef(1), PiObject(0))
  val callResHi: (CallRef, PiObject) = (CallRef(0), PiObject("Hello, World!"))
  val callResErr: (CallRef, TestException) = (CallRef(53141), new TestException)

  val eg1: eg1 = new eg1

  class eg1 extends {

    val piiId: ObjectId = ObjectId.get

    val proc1: AtomicProcess = pai
    val proc2: AtomicProcess = pbi
    val proc3: AtomicProcess = pci

    val arg1: PiObject = PiObject(13)
    val arg2: PiObject = PiObject(1)
    val arg3: PiObject = PiObject(3)

    val r1: (CallRef, PiObject) = (CallRef(0), PiObject((1, 3)))
    val r2: (CallRef, PiObject) = (CallRef(1), PiObject("R1"))
    val r3: (CallRef, PiObject) = (CallRef(2), PiObject("R2"))

    val pNew: PiInstance[ObjectId] = PiInstance(piiId, ri, PiObject(13))

    val rrNew: ReduceRequest = ReduceRequest(pNew, Seq())

    val pInProgress: PiInstance[ObjectId] = pNew.reduce
      .handleThreads((_, _) => true)
      ._2

    val assgnInProgress: Assignment = Assignment(
      pInProgress,
      r1._1,
      pai.name,
      Seq(PiResource(arg1, pai.inputs.head._1))
    )

    val rpeInProgress: RemoteProcessException[ObjectId] =
      RemoteProcessException(piiId, r1._1.id, testException)
    val sfInProgress: SequenceFailure = SequenceFailure(piiId, r1._1, rpeInProgress)

    val srInProgress: SequenceRequest = SequenceRequest(piiId, r1)
    val rrInProgress: ReduceRequest = ReduceRequest(pInProgress, Seq(r1))

    val pFinishing: PiInstance[ObjectId] = pInProgress
      .postResult(r1._1.id, r1._2)
      .reduce
      .handleThreads((_, _) => true)
      ._2

    val assgnFinishing2: Assignment = Assignment(
      pFinishing,
      r2._1,
      pbi.name,
      Seq(PiResource(arg2, pbi.inputs.head._1))
    )

    val assgnFinishing3: Assignment = Assignment(
      pFinishing,
      r3._1,
      pci.name,
      Seq(PiResource(arg3, pci.inputs.head._1))
    )

    val pepeFinishing2: PiFailureAtomicProcessException[ObjectId] =
      PiFailureAtomicProcessException(piiId, r2._1.id, testException)

    val rpeFinishing: RemoteProcessException[ObjectId] =
      RemoteProcessException(piiId, r2._1.id, testException)

    val sfFinishing21: SequenceFailure = SequenceFailure(piiId, r2._1, rpeFinishing)

    val sfFinishing22: SequenceFailure =
      SequenceFailure(Right(pFinishing), Seq((r2._1, null)), Seq(rpeFinishing.event))

    val sfFinishing3: SequenceFailure =
      SequenceFailure(Right(pFinishing), Seq((r2._1, null), r3), Seq(rpeFinishing.event))

    val srFinishing2: SequenceRequest = SequenceRequest(piiId, r2)
    val srFinishing3: SequenceRequest = SequenceRequest(piiId, r3)

    val rrFinishing2: ReduceRequest = ReduceRequest(pFinishing, Seq(r2))
    val rrFinishing3: ReduceRequest = ReduceRequest(pFinishing, Seq(r3))
    val rrFinishing23: ReduceRequest = ReduceRequest(pFinishing, Seq(r2, r3))

    val pCompleted: PiInstance[ObjectId] = pFinishing
      .postResult(r2._1.id, r2._2)
      .postResult(r3._1.id, r3._2)
      .reduce

  }

  def update(pii: PiInstance[ObjectId], part: Int): (PiiHistory, Int) = (PiiUpdate(pii), part)

  val result: (CallRef, PiObject) = (CallRef(0), PiObject(0))

  def seqreq(pii: PiInstance[ObjectId], res: (CallRef, PiObject), part: Int): (PiiHistory, Int) =
    (SequenceRequest(pii.id, res), part)

  def seqfail(pii: PiInstance[ObjectId], ref: CallRef, part: Int): (PiiHistory, Int) =
    (SequenceFailure(pii.id, ref, RemoteProcessException(pii.id, ref.id, new TestException)), part)
}
