package com.workflowfm.pew.stateless.components

import com.workflowfm.pew.stateless.StatelessRouter
import com.workflowfm.pew.{PiEventHandler, PiObject}
import org.bson.types.ObjectId
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext

class StatelessEventHandler[ResultT, MsgT](
    handlers: Seq[PiEventHandler[ObjectId, _]]

  )(
    implicit router: StatelessRouter[MsgT],
    execCtx: ExecutionContext

  ) extends StatelessComponent[MsgT] {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  import com.workflowfm.pew.stateless.StatelessMessages._

  override def receive: PartialFunction[Any, IO[MsgT]] = {
    case Result( pii, _, content ) => content match {
      case failure: Throwable =>
        handlers foreach (_.failure( pii, failure ))
        noResponse

      case _: PiObject =>
        pii.result match {
          case Some( piiRes ) => handlers foreach (_.success( pii, piiRes ))
          case None           => logger.error( "Success with no result" )
        }
        noResponse

      case result =>
        handlers foreach ( _.success( pii, result ) )
        noResponse
    }

    case m => super.receive(m)
  }
}
