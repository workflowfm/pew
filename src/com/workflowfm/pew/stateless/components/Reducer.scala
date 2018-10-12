package com.workflowfm.pew.stateless.components

import com.workflowfm.pew._
import com.workflowfm.pew.execution.ProcessExecutor.{AtomicProcessIsCompositeException, NoResultException, UnknownProcessException}
import com.workflowfm.pew.stateless.CallRef
import com.workflowfm.pew.stateless.StatelessMessages.{AnyMsg, ReduceRequest}
import org.bson.types.ObjectId

import scala.concurrent.ExecutionContext

/** Takes New Process Instances -> Updated Workflow State, and either an immediate result or a list of requested computations
  *   Separated from StatelessExecActor to ensure the WorkflowState message is actually posted before the tasks are executed.
  */
class Reducer(
   implicit exec: ExecutionContext

 ) extends StatelessComponent[ReduceRequest, Seq[AnyMsg]] {

  import com.workflowfm.pew.stateless.StatelessMessages._

  override def respond: ReduceRequest => Seq[AnyMsg] = request
    => piiReduce( request.pii.postResult( request.args.map( a => (a._1.id, a._2)) ) )

  def piiReduce( pii: PiInstance[ObjectId] ): Seq[AnyMsg] = {
    val piiReduced: PiInstance[ObjectId] = pii.reduce

    if (piiReduced.completed) Seq( piiReduced.result match {
      case Some(_) => new ResultSuccess( piiReduced, piiReduced.process.output._1 )
      case None =>    new ResultFailure( piiReduced, NoResultException( piiReduced.id.toString ) )

    }) else {

      val ( toCall, piiReady ) = handleThreads( piiReduced )
      val futureCalls = (toCall map CallRef.apply) zip (toCall flatMap piiReady.piFutureOf)

      val updateMsg = PiiUpdate( piiReady )
      val requests = futureCalls map ( getMessages( piiReady )(_, _) ).tupled

      requests :+ updateMsg
    }
  }

  def handleThreads( piInst: PiInstance[ObjectId] ): ( Seq[Int], PiInstance[ObjectId] ) = {
    piInst.handleThreads( handleThread( piInst ) )
  }

  // Jev, TODO confirm that exceptions here would've bottled the whole evaluation.
  def handleThread( i: PiInstance[ObjectId])( ref: Int, f: PiFuture ): Boolean = {
    f match {
      case PiFuture(name, _, _) => i.getProc(name) match {

        case Some(_: AtomicProcess) => true

        case None =>
          //logger.error("[" + i.id + "] Unable to find process: " + name)
          throw UnknownProcessException( name )

        case Some(_: CompositeProcess) =>
          //logger.error("[" + i.id + "] Executor encountered composite process thread: " + name)
          throw AtomicProcessIsCompositeException( name )
      }
    }
  }

  def getMessages( piReduced: PiInstance[ObjectId] )( ref: CallRef, fut: PiFuture ): AnyMsg = {
    fut match {
      case PiFuture(name, _, args) =>
        piReduced.getProc(name) match {

          // Request that the process be executed.
          case Some(p: AtomicProcess) => Assignment( piReduced, ref, p.name, args )

          // These should never happen! We already checked in the reducer!
          case None    => SequenceFailure( piReduced.id, ref, UnknownProcessException(name) )
          case Some(_) => SequenceFailure( piReduced.id, ref, AtomicProcessIsCompositeException(name) )
        }
    }
  }
}