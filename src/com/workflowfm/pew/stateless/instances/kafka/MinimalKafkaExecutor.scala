package com.workflowfm.pew.stateless.instances.kafka

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.Producer
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.workflowfm.pew._
import com.workflowfm.pew.stateless._
import com.workflowfm.pew.stateless.components.ResultListener
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConnectors
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings
import org.bson.types.ObjectId
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent._

/** Minimal implementation of a KafkaExecutor that needs to be
  * present on the local machine to complete the Executor interface.
  * Other components are required are required to run on the Kafka cluster
  * but need not be situated on the local machine.
  */
class MinimalKafkaExecutor[ResultT](
    processes: PiProcessStore

  ) (
    implicit settings: KafkaExecutorSettings

  ) extends StatelessExecutor[ResultT] {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  import StatelessMessages._
  import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConnectors._

  // Implicit settings.
  implicit val actorSystem: ActorSystem = settings.actorSys
  implicit val executionContext: ExecutionContext = settings.execCtx
  implicit val materializer: Materializer = settings.materializer

  val defaultHandler = new LoggerHandler[ObjectId]( logger )
  val futureHandler = new FuturePiEventHandler[ObjectId, ResultT]
  val handlers = Seq( defaultHandler, futureHandler )

  // Necessary local KafkaComponent instance.
  val eventHandler: ResultListener = ResultListener( defaultHandler, futureHandler )
  val eventHandlerControl: Control = uniqueResultListener( eventHandler )

  def connect( id: ObjectId, process: PiProcess, args: Seq[Any] )
    : ( PiInstance[ObjectId], Future[ResultT] ) = {
    logger.info( "Connected to PiInstance: " + id.toString )

    val pii = PiInstance( id, process, args map PiObject.apply: _* )
    defaultHandler.start( pii )
    val result = futureHandler.start( pii )

    ( pii, result )
  }

  def executeWith( id: ObjectId, process: PiProcess, args: Seq[Any] ): Future[ResultT] = {
    logger.info("Starting execution: " + process.toString )

    val ( pii, result ) = connect( id, process, args )

    // Locally send the initial reduce request.
    logger.info( "Seeding initial 'ReduceRequest'." )
    sendSingleMessage( ReduceRequest( pii, Seq() ) )

    result
  }

  override def execute( process: PiProcess, args: Seq[Any] ): Future[ResultT] = {
    executeWith( new ObjectId, process, args )
  }

  // TODO: What does this do? Its not handled
  override def simulationReady: Boolean = false

  override def shutdown: Future[Done] = KafkaConnectors.shutdown( eventHandlerControl )
}
