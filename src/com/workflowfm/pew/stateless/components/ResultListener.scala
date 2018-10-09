package com.workflowfm.pew.stateless.components

import com.workflowfm.pew.stateless.StatelessMessages.PiiResult
import com.workflowfm.pew.{PiEventHandler, PiObject}
import org.bson.types.ObjectId

import scala.language.implicitConversions

class ResultListener(
    handlers: Seq[PiEventHandler[ObjectId, _]]

  ) extends StatelessComponent[PiiResult[Any], Unit] {

  import com.workflowfm.pew.stateless.StatelessMessages._

  override def respond: PiiResult[Any] => Unit = {
    case PiiResult( pii, content ) =>
      content match {
        case failure: Throwable =>
          handlers foreach (_.failure( pii, failure ))

        case _: PiObject =>
          pii.result match {
            case Some( piiRes ) => handlers foreach (_.success( pii, piiRes ))
            case None           => System.err.println( "Success with no result" )
          }

        case result =>
          handlers foreach ( _.success( pii, result ) )
      }
  }
}

object ResultListener {
  def apply( handlers: PiEventHandler[ObjectId, _]* ): ResultListener
    = new ResultListener( handlers.toSeq )
}