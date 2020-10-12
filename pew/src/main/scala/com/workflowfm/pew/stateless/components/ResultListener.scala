package com.workflowfm.pew.stateless.components

import scala.concurrent.ExecutionContext
import scala.language.implicitConversions

import org.bson.types.ObjectId

import com.workflowfm.pew.stateless.StatelessMessages.PiiLog
import com.workflowfm.pew.stream.{ PiEventHandler, PiObservable, SimplePiObservable }

class ResultListener(implicit val executionContext: ExecutionContext = ExecutionContext.global)
    extends StatelessComponent[PiiLog, Unit]
    with PiObservable[ObjectId]
    with SimplePiObservable[ObjectId] {

  override def respond: PiiLog => Unit = msg => publish(msg.event)
}

object ResultListener {

  def apply(handlers: PiEventHandler[ObjectId]*): ResultListener = {
    val newResultListener: ResultListener = new ResultListener
    handlers foreach newResultListener.subscribe
    newResultListener
  }

}
