package com.workflowfm.pew.stateless.components

import com.workflowfm.pew._
import com.workflowfm.pew.execution.ProcessExecutor
import com.workflowfm.pew.stateless.{CallRef, StatelessRouter}
import org.bson.types.ObjectId
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext

/** Takes New Process Instances -> Updated Workflow State, and either an immediate result or a list of requested computations
  *   Separated from StatelessExecActor to ensure the WorkflowState message is actually posted before the tasks are executed.
  */
class StatelessReducer[MsgT](
   implicit router: StatelessRouter[MsgT],
   exec: ExecutionContext

 ) extends StatelessComponent[MsgT] {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  import com.workflowfm.pew.stateless.StatelessMessages._

  def piiReduce( request: ReduceRequest ): IO[MsgT] = {
    piiReduce( request.pii.postResult( request.args.map( a => (a._1.id, a._2)) ) )
  }

  def piiReduce( pii: PiInstance[ObjectId] ): IO[MsgT] = {
    val piiReduced: PiInstance[ObjectId] = pii.reduce

    if (piiReduced.completed) piiReduced.result match {
      case Some( _ ) =>
        send( new ResultSuccess( piiReduced, piiReduced.process.output._1 ) )
      case None =>
        send( new ResultFailure( piiReduced, ProcessExecutor.NoResultException(piiReduced.id.toString) ) )

    } else {
      val ( toCall, piiReady ) = handleThreads( piiReduced )
      val futureCalls = (toCall map CallRef.apply) zip (toCall flatMap piiReady.piFutureOf)

      val updateMsg = PiiUpdate( piiReady )
      val requests = futureCalls map ( getMessages( piiReady )(_, _) ).tupled

      sendAll( requests :+ updateMsg )
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
          logger.error("[" + i.id + "] Unable to find process: " + name)
          throw ProcessExecutor.UnknownProcessException(name)

        case Some(_: CompositeProcess) =>
          logger.error("[" + i.id + "] Executor encountered composite process thread: " + name)
          throw ProcessExecutor.AtomicProcessIsCompositeException(name)
      }
    }
  }

  def getMessages( piReduced: PiInstance[ObjectId] )( ref: CallRef, fut: PiFuture ): Any = {
    fut match {
      case PiFuture(name, _, args) =>
        piReduced.getProc(name) match {

          // Request that the process be executed.
          case Some(p: AtomicProcess)
            => Assignment( piReduced, ref, done = false, p.name, args )

          // These should never happen! We already checked in the reducer!
          case None
            => new ResultFailure( piReduced, ref, ProcessExecutor.UnknownProcessException(name) )
          case Some(_: CompositeProcess)
            => new ResultFailure( piReduced, ref, ProcessExecutor.AtomicProcessIsCompositeException(name) )
        }
    }
  }

  override def receive: PartialFunction[Any, IO[MsgT]] = {
    case m: ReduceRequest => piiReduce( m )
    case m => super.receive(m)
  }
}