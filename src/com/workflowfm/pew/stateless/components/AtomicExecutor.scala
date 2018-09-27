package com.workflowfm.pew.stateless.components

import com.workflowfm.pew._
import com.workflowfm.pew.stateless.StatelessMessages._

import scala.concurrent._
import scala.util.{Failure, Success}

/** Runs AtomicProcesses and handles all necessary communication with the various stateless workflow actors.
  *
  */
class AtomicExecutor(implicit exec: ExecutionContext )
  extends StatelessComponent[Assignment, Future[AnyMsg]] {

  def getProc( pii: PiInstance[_], processName: String ): AtomicProcess
    = pii.getProc( processName ) match {

    case Some( proc: AtomicProcess ) => proc
  }

  override def respond: Assignment => Future[AnyMsg] = {
    case Assignment( pii, callRef, name, args ) =>

      System.out.println(s"Started process '$name' - callRef_${callRef.id}")
      getProc(pii, name).run( args map (_.obj) ).transformWith {

        case Success(res) =>
          Future.successful( SequenceRequest( pii.id, (callRef, res) ) )

        case Failure(e) =>
          System.err.println(s"Error running process: $name")
          e.printStackTrace()
          Future.successful( new ResultFailure(pii, callRef, e) )
      }
  }
}