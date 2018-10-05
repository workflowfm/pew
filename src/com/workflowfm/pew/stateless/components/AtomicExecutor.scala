package com.workflowfm.pew.stateless.components

import com.workflowfm.pew._
import com.workflowfm.pew.execution.ProcessExecutor.UnknownProcessException
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.stateless.components.AtomicExecutor.ErrorHandler

import scala.concurrent._

/** Runs AtomicProcesses and handles all necessary communication with the various stateless workflow actors.
  *
  */
class AtomicExecutor( errHandler: ErrorHandler )( implicit exec: ExecutionContext )
  extends StatelessComponent[Assignment, Future[AnyMsg]] {

  import AtomicExecutor._

  override def respond: Assignment => Future[AnyMsg] = {
    case Assignment( pii, ref, name, args ) =>

      val callName: String = s"ProcExe($name: ${ref.id})"
      lazy val handling: Handling = Handling( callName, run( errHandler ), fail )

      def run( err: ErrorHandler )(): Future[AnyMsg] = {

        val proc: Option[AtomicProcess]
          = pii.getProc( name ).collect({ case ap: AtomicProcess => ap })

        try {
          proc
          .getOrElse( throw UnknownProcessException( name ) )
          .run( args map (_.obj) )
          .map( res => SequenceRequest( pii.id, (ref, res) ) )
          .recoverWith( err( proc.orNull )( handling ) )

        } catch {
          case t: Throwable =>
            err( proc.orNull )( handling )( PreExecutionException(t) )
        }
      }

      def fail( t: Throwable ): Future[ResultFailure]
        = Future.successful( new ResultFailure(pii, ref, t) )

      println(s"Started: '$callName'.")
      handling.run()
  }
}

object AtomicExecutor {

  def apply( errHandler: ErrorHandler = _ => defaultHandler() )( implicit exec: ExecutionContext )
    : AtomicExecutor = new AtomicExecutor( errHandler )

  case class PreExecutionException( t: Throwable ) extends Throwable

  case class Handling(
    name: String,
    run: () => Future[AnyMsg],
    fail: Throwable => Future[ResultFailure]
  )

  type HandlerLogic = Handling => PartialFunction[Throwable, Future[AnyMsg]]
  type ErrorHandler = AtomicProcess => HandlerLogic

  /** Retry execution in this AtomicProcessExecutor for `maxAttempts` before
    * resorting to a backup handler logic.
    *
    * @param maxAttempts Number of times to unsuccessfully call `run` before using backup.
    * @param backup Backup ErrorHandler to use in even of hitting attempt cap.
    */
  class RetryHandler( var maxAttempts: Int, backup: HandlerLogic ) extends HandlerLogic {
    override def apply( handling: Handling ): PartialFunction[Throwable, Future[AnyMsg]] = {
      case t: Throwable =>
        if ( maxAttempts > 1 ) {
          maxAttempts -= 1
          handling.run()

        } else backup( handling )( t )
    }
  }

  /** Cancel the execution of this workflow by sending a `ResultFailure` message.
    */
  def cancelHandler: HandlerLogic = handling => {
    case t: Throwable =>
      System.err.println(s"AtomicProcessExecutor: Canceling '${handling.name}'.")
      t.printStackTrace()
      handling.fail( t )
  }

  /** Retry execution 3 times before cancelling workflow.
    */
  def defaultHandler(): HandlerLogic = new RetryHandler( 3, cancelHandler )

}