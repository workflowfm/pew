package com.workflowfm.pew.stateless.instances

import akka.actor._
import com.workflowfm.pew._
import com.workflowfm.pew.stateless.{FuturePiEventHandler, StatelessExecutor, StatelessRouter}
import com.workflowfm.pew.stateless.components.{StatelessAtomicProcess, StatelessComponent, StatelessEventHandler, StatelessReducer}
import org.bson.types.ObjectId
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent._

class StatelessActor( component: StatelessComponent[Unit] )
  extends Actor {

  override def receive: Receive = {
    case msg: Any => component.receive( msg )
  }
}

class AkkaStatelessExecutor[ResultT](
    processes: PiProcessStore,

  ) (
    implicit val system: ActorSystem,
    val execCtx: ExecutionContext = ExecutionContext.global

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

  def makeActor( name: String, component: StatelessComponent[Unit] ): ActorRef = {
    system.actorOf( Props( new StatelessActor( component ) ), name=name )
  }

  // Akka Actors
  val reducerActor: ActorRef      = makeActor( "Reducer", reducer )
  val procExecutorActor: ActorRef = makeActor( "Executor", procExecutor )
  val handlerActor: ActorRef      = makeActor( "Handler", handler )

  override def route( msg: Any ): Unit = {
    logger.trace( "Routing message: " + msg.toString )
    msg match {
      // case PiiUpdate(_,_) => sequencerActor ! msg
      case Assignment(_,_,_,_,_) => procExecutorActor ! msg
      case Result(_,_,_) => handlerActor ! msg
    }
  }

  override def execute( process: PiProcess, args: Seq[Any] ): Future[ResultT] = {
    val pii = PiInstance( new ObjectId, process, args map PiObject.apply: _* )
    val result = futureHandler.start( pii )
    // Future { route( StatelessMessages.ReduceRequest( pii, None ) ) }
    result
  }

  // TODO: What does this do? Its not handled
  override def simulationReady: Boolean = false

}
