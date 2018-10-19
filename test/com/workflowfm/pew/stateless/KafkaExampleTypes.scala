package com.workflowfm.pew.stateless

import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew._
import org.bson.types.ObjectId

//noinspection TypeAnnotation
trait KafkaExampleTypes extends KafkaTests {

  val p1 = PiInstance( ObjectId.get, pbi, PiObject(1) )
  val p2 = PiInstance( ObjectId.get, pbi, PiObject(1) )

  val testException = RemoteExecutorException("test")

  val callRes0 = ( CallRef(1), PiObject(0) )
  val callResHi = ( CallRef(0), PiObject("Hello, World!") )
  val callResErr = ( CallRef(53141), RemoteExecutorException("Argh!") )

  val eg1 = new {

    val piiId: ObjectId = ObjectId.get

    val proc1: AtomicProcess = pai
    val proc2: AtomicProcess = pbi
    val proc3: AtomicProcess = pci

    val arg1 = PiObject(13)
    val arg2 = PiObject(1)
    val arg3 = PiObject(3)

    val r1 = ( CallRef(0), PiObject((1, 3)) )
    val r2 = ( CallRef(1), PiObject("R1") )
    val r3 = ( CallRef(2), PiObject("R2") )

    val pNew: PiInstance[ObjectId]
      = PiInstance( piiId, ri, PiObject(13) )

    val rrNew: ReduceRequest = ReduceRequest( pNew, Seq() )

    val pInProgress: PiInstance[ObjectId]
      = pNew
        .reduce
        .handleThreads((_, _) => true)._2

    val assgnInProgress
      = Assignment(
        pInProgress, r1._1, pai.name,
        Seq( PiResource( arg1, pai.inputs.head._1 ) )
      )

    val sfInProgress = SequenceFailure( piiId, r1._1, testException )

    val srInProgress = SequenceRequest( piiId, r1 )
    val rrInProgress = ReduceRequest( pInProgress, Seq( r1 ) )

    val pFinishing: PiInstance[ObjectId]
      = pInProgress
        .postResult( r1._1.id, r1._2 )
        .reduce
        .handleThreads((_, _) => true)._2

    val assgnFinishing2
      = Assignment(
        pFinishing, r2._1, pbi.name,
        Seq( PiResource( arg2, pbi.inputs.head._1 ) )
      )

    val assgnFinishing3
      = Assignment(
        pFinishing, r3._1, pci.name,
        Seq( PiResource( arg3, pci.inputs.head._1 ) )
      )

    val pepeFinishing2 = PiEventProcessException( piiId, r2._1.id, testException )

    val sfFinishing21 = SequenceFailure( piiId, r2._1, testException )
    val sfFinishing22 = SequenceFailure( pFinishing, Seq(), Seq( pepeFinishing2 ) )
    val sfFinishing3 = SequenceFailure( pFinishing, Seq( r3 ), Seq( pepeFinishing2 ) )

    val srFinishing2 = SequenceRequest( piiId, r2 )
    val srFinishing3 = SequenceRequest( piiId, r3 )

    val rrFinishing2 = ReduceRequest( pFinishing, Seq( r2 ) )
    val rrFinishing3 = ReduceRequest( pFinishing, Seq( r3 ) )
    val rrFinishing23 = ReduceRequest( pFinishing, Seq( r2, r3 ) )

    val pCompleted: PiInstance[ObjectId]
      = pFinishing
        .postResult( r2._1.id, r2._2 )
        .postResult( r3._1.id, r3._2 )
        .reduce

  }

  def update( pii: PiInstance[ObjectId], part: Int ): (PiiHistory, Int)
    = ( PiiUpdate( pii ), part )

  val result = (CallRef(0), PiObject(0))

  def seqreq( pii: PiInstance[ObjectId], res: (CallRef, PiObject), part: Int ): (PiiHistory, Int)
    = ( SequenceRequest( pii.id, res ), part )

  def seqfail( pii: PiInstance[ObjectId], ref: CallRef, part: Int ): (PiiHistory, Int)
    = ( SequenceFailure( pii.id, ref, RemoteExecutorException("test") ), part )
}
