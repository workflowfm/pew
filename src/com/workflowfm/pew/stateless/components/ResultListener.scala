package com.workflowfm.pew.stateless.components

import com.workflowfm.pew.stateless.StatelessMessages.PiiLog
import com.workflowfm.pew._
import org.bson.types.ObjectId

import scala.language.implicitConversions

class ResultListener
  extends StatelessComponent[PiiLog, Unit]
  with PiObservable[ObjectId] with SimplePiObservable[ObjectId] {

  override def respond: PiiLog => Unit = msg => publish( msg.event )
}

object ResultListener {

  def apply( handlers: PiEventHandler[ObjectId]* ): ResultListener = {
    val newResultListener: ResultListener = new ResultListener
    handlers foreach newResultListener.subscribe
    newResultListener
  }

}