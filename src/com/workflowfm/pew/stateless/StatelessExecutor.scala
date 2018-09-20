package com.workflowfm.pew.stateless

import com.workflowfm.pew.execution._
import com.workflowfm.pew.{PiInstance, _}
import org.slf4j.Logger

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, _}

// WrapperPiEventHandler(logger, base)

class FuturePiEventHandler[KeyT, ResultT](
   implicit val execCtx: ExecutionContext = ExecutionContext.global
  ) extends PiEventHandler[KeyT, Future[ResultT]] {

  var promises:Map[KeyT, Promise[Any]] = Map()

  override def start(i: PiInstance[KeyT]): Future[ResultT] = promises synchronized {
    val promise = Promise[Any]()
    promises += (i.id -> promise)
    promise.future.map( anyVal => anyVal.asInstanceOf[ResultT] )
  }

  override def success(i: PiInstance[KeyT], res: Any): Unit = promises synchronized {
    promises.get(i.id) match {
      case None => Unit
      case Some(p) => p.success(res)
    }
    promises = promises - i.id
  }

  override def failure(i: KeyT, reason: Throwable): Unit = promises synchronized {
    promises.get(i) match {
      case None => Unit
      case Some(p) => p.failure(reason)
    }
  }

  override def failure(i: PiInstance[KeyT], reason: Throwable): Unit = promises synchronized {
    promises.get(i.id) match {
      case None => Unit
      case Some(p) => p.failure(reason)
    }
  }
}

class LoggerHandler[T]( logger: Logger )
  extends PiEventHandler[T,Unit] {

  val piiConcerns: mutable.HashSet[T] = new mutable.HashSet

  override def start( i: PiInstance[T] ): Unit = {
    piiConcerns.add( i.id )
    logger.info(" === INITIAL STATE " + i.id + " === \n" + i.state + "\n === === === === === === === ===")
  }

  override def success( i: PiInstance[T], res: Any) = {
    if ( piiConcerns contains i.id ) {
      logger.info(" === FINAL STATE " + i.id + " === \n" + i.state + "\n === === === === === === === ===")
      logger.info(" === RESULT FOR " + i.id + ": " + res)
    }
  }

  override def failure( i: PiInstance[T], reason: Throwable ) = {
    if ( piiConcerns contains i.id ) {
      logger.error(" === FINAL STATE " + i.id + " === \n" + i.state + "\n === === === === === === === ===")
      logger.error(" === FAILED: " + i.id + " ! === Exception: " + reason)
      reason.printStackTrace()
    }
  }

  override def failure( i: T, reason: Throwable ) = {
    if ( piiConcerns contains i ) {
      logger.error(" === FAILED: " + i + " ! === Exception: " + reason)
      reason.printStackTrace()
    }
  }
}

/** Boots up necessary Workflow actors for a "stateless" (no RAM) workflow execution.
  *
  */
abstract class StatelessExecutor[ResultT]
  extends ProcessExecutor[ResultT] {

  // def shutdown: Future[Done]
}

// TODO, Should really be (PiInstance, CallRefID: Int)
case class CallRef( id: Int )
object CallRef {
  val IDLE: CallRef = CallRef( 0 )
}

