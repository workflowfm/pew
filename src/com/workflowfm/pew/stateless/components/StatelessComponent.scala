package com.workflowfm.pew.stateless.components

import java.util.UUID

import com.workflowfm.pew.stateless.StatelessRouter
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent._
import scala.util._

abstract class StatelessComponent[MsgOut](
    implicit router: StatelessRouter[MsgOut],
    implicit val exec: ExecutionContext

  ) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val uuid: UUID = UUID.randomUUID()

  type IO[T] = Future[Seq[T]]

  def sendAll( msgs: Seq[Any] ): IO[MsgOut] =
    Future.successful( msgs map router.route )

  val noResponse: IO[MsgOut] = sendAll( Seq() )
  def send( msg: Any ): IO[MsgOut] = sendAll( Seq( msg ) )

  def thenSend[T]( job: Future[T], handlerFn: Try[T] => IO[MsgOut] ): IO[MsgOut] = {
    job.transform( Success(_) ) // Create stream of Try to be handled.
    .flatMap( handlerFn )       // Join all streams together with necessary handling inside handlerFn.
  }

  def receive: PartialFunction[Any, IO[MsgOut]] = {
    case m =>
      logger.info( "Unhandled message: {}", m.toString )
      noResponse
  }
}