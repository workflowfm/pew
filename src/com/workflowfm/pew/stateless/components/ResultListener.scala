package com.workflowfm.pew.stateless.components

import com.workflowfm.pew.stateless.StatelessMessages.PiiResult
import com.workflowfm.pew._
import org.bson.types.ObjectId

import scala.language.implicitConversions

class ResultListener
  extends StatelessComponent[PiiResult[Any], Unit]
  with PiObservable[ObjectId] with SimplePiObservable[ObjectId] {

  override def respond: PiiResult[Any] => Unit = piiResult => {
    val pii: PiInstance[ObjectId] = piiResult.pii
    publish( piiResult.res match {

      case failure: Throwable => PiEventException( pii.id, failure )

      case _: PiObject =>
        piiResult.pii.result match {
          case Some( piiRes ) => PiEventResult( pii, piiRes )
          case None           => PiFailureNoResult( pii )
        }

      case result => PiEventResult( pii, result )
    })
  }
}

object ResultListener {

  def apply( handlers: PiEventHandler[ObjectId]* ): ResultListener = {
    val newResultListener: ResultListener = new ResultListener
    handlers foreach newResultListener.subscribe
    newResultListener
  }

}