package com.workflowfm.pew.stateless.instances

import com.workflowfm.pew.stateless.components.{StatelessAtomicProcess, StatelessEventHandler, StatelessReducer}
import com.workflowfm.pew.stateless.{FuturePiEventHandler, StatelessExecutor, StatelessRouter}
import com.workflowfm.pew.{PiInstance, _}
import org.bson.types._
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}

class NaiveStatelessExecutor[ResultT](
     processes: PiProcessStore,
     implicit val execCtx: ExecutionContext = ExecutionContext.global

  ) extends StatelessExecutor[ResultT]
  with StatelessRouter[Unit] {

  import com.workflowfm.pew.stateless.StatelessMessages._

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val exec: StatelessRouter[Unit] = this

  // Event Handlers
  val defaultHandler = new DefaultHandler[ObjectId]
  val futureHandler = new FuturePiEventHandler[ObjectId, ResultT]
  val handlers = Seq( defaultHandler, futureHandler )

  // Stateless Components
  val reducer = new StatelessReducer
  val procExecutor = new StatelessAtomicProcess
  val handler = new StatelessEventHandler[Future[ResultT], Unit]( handlers )

  override def route( msg: Any ): Unit = {
    logger.trace( "Routing message: " + msg.toString )
    msg match {
      case Assignment(_,_,_,_,_) => procExecutor.receive(msg)
      case Result(_,_,_) => handler.receive(msg)
    }
  }

  override def execute( process: PiProcess, args: Seq[Any] ): Future[ResultT] = {
    val pii = PiInstance( new ObjectId, process, args map PiObject.apply: _* )
    val result = futureHandler.start( pii )
    result
  }

  // TODO: What does this do? Its not handled
  override def simulationReady: Boolean = false
}
