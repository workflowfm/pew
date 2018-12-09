package com.workflowfm.pew.stateless

import akka.Done
import com.workflowfm.pew.stream.PiObservable
import com.workflowfm.pew.execution._

import scala.concurrent.duration.Duration
import scala.concurrent.{Future, _}

/** Boots up necessary Workflow actors for a "stateless" (no RAM) workflow execution.
  *
  */
abstract class StatelessExecutor[KeyT]
  extends ProcessExecutor[KeyT] { this:PiObservable[KeyT] =>

  def shutdown: Future[Done]

  final def syncShutdown( timeout: Duration = Duration.Inf ): Done
    = Await.result( shutdown, timeout )
}

// TODO, Should really be (PiInstance, CallRefID: Int)
case class CallRef( id: Int )
object CallRef {
  val IDLE: CallRef = CallRef( 0 )
}

