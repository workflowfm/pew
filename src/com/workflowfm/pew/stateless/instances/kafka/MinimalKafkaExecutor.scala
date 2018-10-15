package com.workflowfm.pew.stateless.instances.kafka

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.Materializer
import com.workflowfm.pew._
import com.workflowfm.pew.stateless._
import com.workflowfm.pew.stateless.components.ResultListener
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConnectors
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings
import org.bson.types.ObjectId
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent._

/** Minimal implementation of a KafkaExecutor that needs to be
  * present on the local machine to complete the Executor interface.
  * Other components are required are required to run on the Kafka cluster
  * but need not be situated on the local machine.
  */
class MinimalKafkaExecutor( implicit settings: KafkaExecutorSettings )
  extends StatelessExecutor[ObjectId] with DelegatedPiObservable[ObjectId] {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  import StatelessMessages._
  import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConnectors._

  // Implicit settings.
  override implicit val context: ExecutionContext = settings.execCtx

  implicit val actorSystem: ActorSystem = settings.actorSys
  implicit val materializer: Materializer = settings.mat

  protected var piiStore: PiInstanceStore[ObjectId] = SimpleInstanceStore()

  /**
    * Initializes a PiInstance for a process execution.
    * This is always and only invoked before a {@code start}, hence why it is protected.
    * This separation gives a chance to PiEventHandlers to subscribe before execution starts.
    * @param process The (atomic or composite) PiProcess to be executed
    * @param args The PiObject arguments to be passed to the process
    * @return A Future with the new unique ID that was generated
    */
  override def init( process: PiProcess, args: Seq[PiObject] ): Future[ObjectId] = {
    val piiId = ObjectId.get
    piiStore = piiStore.put( PiInstance( piiId, process, args:_* ) )
    Future.successful( piiId )
  }

  override def start( id: ObjectId ): Unit = {
    piiStore.get( id ) match {
      case None =>
        eventHandler.publish( PiFailureNoSuchInstance( id ) )

      case Some( pii ) =>
        logger.info( "Seeding initial 'ReduceRequest'." )
        sendMessages( ReduceRequest( pii, Seq() ) )
    }
  }

  // Necessary local KafkaComponent instance.
  val eventHandler: ResultListener = new ResultListener
  override val worker: PiObservable[ObjectId] = eventHandler
  val eventHandlerControl: Control = uniqueResultListener( eventHandler )

  override def shutdown: Future[Done] = KafkaConnectors.shutdown( eventHandlerControl )
}
