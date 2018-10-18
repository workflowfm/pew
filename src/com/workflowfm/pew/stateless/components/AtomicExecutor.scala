package com.workflowfm.pew.stateless.components

import com.workflowfm.pew._
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

      def fail(t: Throwable): Future[SequenceFailure]
        = Future.successful( SequenceFailure(pii.id, ref, t) )

      try {
        val proc: AtomicProcess = pii.getAtomicProc( name )

        val callName: String = s"ProcExe($name: ${ref.id})"
        lazy val handling: Handling = Handling(callName, run(errHandler), fail)

        def run(err: ErrorHandler)(): Future[AnyMsg] = {
          println(s"Running: '$callName'.")

          try {
            proc
            .run(args map (_.obj))
            .map(res => SequenceRequest(pii.id, (ref, res)))
            .recoverWith(err(proc)(handling))

          } catch {
            case t: Throwable => err(proc)(handling)(PreExecutionException(t))
          }
        }

        println(s"Started: '$callName'.")
        handling.run()

      } catch {
        case ex: UnknownProcessException[_]           => fail( ex )
        case ex: AtomicProcessIsCompositeException[_] => fail( ex )
        case ex: Exception                            => fail( ex )
      }
  }
}

object AtomicExecutor {

  def apply( errHandler: ErrorHandler = _ => cancelHandler )( implicit exec: ExecutionContext )
    : AtomicExecutor = new AtomicExecutor( errHandler )

  case class PreExecutionException( t: Throwable ) extends Throwable

  case class Handling(
    name: String,
    run: () => Future[AnyMsg],
    fail: Throwable => Future[SequenceFailure]
  )

  type HandlerLogic = Handling => PartialFunction[Throwable, Future[AnyMsg]]
  type ErrorHandler = AtomicProcess => HandlerLogic

  // TODO: Rework implementation so `maxAttempts` actually does something.
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
          println(s"Retrying: ${handling.name}, ${maxAttempts - 1} remaining.")
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