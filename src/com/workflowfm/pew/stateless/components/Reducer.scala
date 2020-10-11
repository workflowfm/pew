package com.workflowfm.pew.stateless.components

import scala.concurrent.ExecutionContext

import org.bson.types.ObjectId

import com.workflowfm.pew._
import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.StatelessMessages.{ AnyMsg, ReduceRequest }

/** Takes New Process Instances -> Updated Workflow State, and either an immediate result or a list of requested computations
  *   Separated from StatelessExecActor to ensure the WorkflowState message is actually posted before the tasks are executed.
  */
class Reducer(
    implicit exec: ExecutionContext
) extends StatelessComponent[ReduceRequest, Seq[AnyMsg]] {

  import com.workflowfm.pew.stateless.StatelessMessages._

  override def respond: ReduceRequest => Seq[AnyMsg] = request =>
    piiReduce(request.pii.postResult(request.args.map(a => (a._1.id, a._2))))

  def piiReduce(pii: PiInstance[ObjectId]): Seq[AnyMsg] = {
    val piiReduced: PiInstance[ObjectId] = pii.reduce

    if (piiReduced.completed) Seq(piiReduced.result match {
      case Some(result) => PiiLog(PiEventResult(piiReduced, result))
      case None => PiiLog(PiFailureNoResult(piiReduced))

    })
    else {

      val (toCall, piiReady) = handleThreads(piiReduced)
      val futureCalls = (toCall map CallRef.apply) zip (toCall flatMap piiReady.piFutureOf)

      val updateMsg = PiiUpdate(piiReady)
      val requests = futureCalls flatMap (getMessages(piiReady)(_, _)).tupled

      requests :+ updateMsg
    }
  }

  def handleThreads(piInst: PiInstance[ObjectId]): (Seq[Int], PiInstance[ObjectId]) = {
    piInst.handleThreads(handleThread(piInst))
  }

  // Jev, TODO confirm that exceptions here would've bottled the whole evaluation.
  def handleThread(i: PiInstance[ObjectId])(ref: Int, f: PiFuture): Boolean = {
    f match {
      case PiFuture(name, _, _) =>
        i.getProc(name) match {

          case Some(_: MetadataAtomicProcess) => true

          case None => throw UnknownProcessException(i, name)
          case Some(_) => throw AtomicProcessIsCompositeException(i, name)
        }
    }
  }

  def getMessages(piReduced: PiInstance[ObjectId])(ref: CallRef, fut: PiFuture): Seq[AnyMsg] = {
    val piiId: ObjectId = piReduced.id
    fut match {
      case PiFuture(name, _, args) =>
        piReduced.getProc(name) match {

          // Request that the process be executed.
          case Some(p: MetadataAtomicProcess) =>
            Seq(
              Assignment(piReduced, ref, p.name, args),
              PiiLog(PiEventCall(piiId, ref.id, p, args map (_.obj)))
            )

          // These should never happen! We already checked in the reducer!
          case None => Seq(SequenceFailure(piiId, ref, UnknownProcessException(piReduced, name)))
          case Some(_) =>
            Seq(SequenceFailure(piiId, ref, AtomicProcessIsCompositeException(piReduced, name)))
        }
    }
  }
}
