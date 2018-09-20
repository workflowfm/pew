package com.workflowfm.pew.stateless.components

import com.workflowfm.pew._
import com.workflowfm.pew.stateless.StatelessRouter
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent._
import scala.util.{Failure, Success}

/** Runs AtomicProcesses and handles all necessary communication with the various stateless workflow actors.
  *
  */
class StatelessAtomicProcess[MsgT, ResultT](
    implicit router: StatelessRouter[MsgT],
    exec: ExecutionContext

  ) extends StatelessComponent[MsgT] {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  import com.workflowfm.pew.stateless.StatelessMessages._

  // TODO WRAP IN CRITICAL SECTION
  // The actors currently executing process call.
  // var currRef: CallRef = CallRef.IDLE

  def getAtomicProcess( pii: PiInstance[_], processName: String ): AtomicProcess
    = pii.getProc( processName ) match {

    case Some( proc: AtomicProcess ) => proc
  }

  override def receive: PartialFunction[Any, IO[MsgT]] = {
    case Assignment( pii, callRef, done, processName, args )
      => if (!done) {
        logger.info("Started process '{}' - callRef_{}", processName, callRef.id )
        val proc: AtomicProcess = getAtomicProcess( pii, processName )

        thenSend[PiObject]( proc.run( args map (_.obj) ), {

          // Jev, sending directly to reducer, handler.success only called on immediate completion.
          // case Success(result)  => send( new StatelessExecutor.Success( pii, callRef, res ) )
          case Success(res) => send( SequenceRequest( pii.id, (callRef, res) ) )

          case Failure(e) => {
            logger.error( "Error running process: " + processName )
            e.printStackTrace()
            send( new ResultFailure( pii, callRef, e ) )
          }
        })

    } else noResponse

    /* Jev, Version that only allows a single AtomicProcess to run at once.
    => if ( (newActorId contains ourUuid) && callRef != currRef ) {
      if (currRef == CallRef.IDLE) {
        currRef = callRef
        process.run( args map (_.obj) ).onComplete {
          case Success(result)  => send( new StatelessExecutor.Result( piId, callRef )(result) )
          case Failure(e)       => send( new StatelessExecutor.Failure( piId, callRef )( e ) )
        }
      } else {
        // Somethings going wrong - don't interrupt a running task though.
        // TODO: some handling or allow multiple processes on the same actor.
      }
    } */

    case m => super.receive(m)
  }
}