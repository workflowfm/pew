package com.workflowfm.pew.execution

import com.workflowfm.pew.{PiProcess, PromiseHandler}
import scala.concurrent.Future

/** Implements the FutureExecutor interface for any ProcessExecutor.
  *
  * @param exec The wrapped ProcessExecutor.
  */
class FutureExecutorAdapter[T]( private val exec: ProcessExecutor[T] )
  extends FutureExecutor {

  override def execute(process: PiProcess, args: Seq[Any]): Future[PromiseHandler.ResultT]
    = Future.successful( exec.execute( process, args ) )

  override def simulationReady: Boolean = exec.simulationReady
}

object FutureExecutorAdapter {

  implicit def apply[T]( exec: ProcessExecutor[T] ): FutureExecutor
    = new FutureExecutorAdapter[T]( exec )

}
