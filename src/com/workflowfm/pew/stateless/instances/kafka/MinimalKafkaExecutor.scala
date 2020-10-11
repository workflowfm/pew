package com.workflowfm.pew.stateless.instances.kafka

import scala.concurrent._

import akka.Done
import akka.actor.ActorSystem
import akka.stream.Materializer
import org.bson.types.ObjectId
import org.slf4j.{ Logger, LoggerFactory }

import com.workflowfm.pew._
import com.workflowfm.pew.stateless._
import com.workflowfm.pew.stateless.components.ResultListener
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConnectors
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorEnvironment
import com.workflowfm.pew.stream.{ DelegatedPiObservable, PiObservable }

/** Minimal implementation of a KafkaExecutor that needs to be
  * present on the local machine to complete the Executor interface.
  * Other components are required are required to run on the Kafka cluster
  * but need not be situated on the local machine.
  */
class MinimalKafkaExecutor(implicit val environment: KafkaExecutorEnvironment)
    extends StatelessExecutor[ObjectId]
    with DelegatedPiObservable[ObjectId] {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  import StatelessMessages._

  import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConnectors._

  // Implicit settings.
  implicit override val executionContext: ExecutionContext = environment.context
  implicit val actorSystem: ActorSystem = environment.actors
  implicit val materializer: Materializer = environment.materializer

  protected var piiStore: PiInstanceStore[ObjectId] = SimpleInstanceStore()

  override def init(instance: PiInstance[_]): Future[ObjectId] = {
    if (isShutdown) throw new ShutdownExecutorException("`init` was called.")

    val piiId = ObjectId.get
    piiStore = piiStore.put(instance.copy(id = piiId))
    Future.successful(piiId)
  }

  override def start(id: ObjectId): Unit = {
    if (isShutdown) throw new ShutdownExecutorException("`start` was called.")

    piiStore.get(id) match {
      case None =>
        eventHandler.publish(PiFailureNoSuchInstance(id))

      case Some(pii) =>
        logger.info("Seeding initial 'ReduceRequest'.")
        sendMessages(
          ReduceRequest(pii, Seq()),
          PiiLog(PiEventStart(pii))
        )
    }
  }

  // Necessary local KafkaComponent instance.
  val eventHandler: ResultListener = new ResultListener
  override val worker: PiObservable[ObjectId] = eventHandler

  val eventHandlerControl: DrainControl = uniqueResultListener(eventHandler)
  lazy val allControls: Seq[DrainControl] = Seq(eventHandlerControl)

  override def shutdown: Future[Done] = {
    KafkaConnectors
      .drainAndShutdownAll(allControls)
      .flatMap(_ => environment.actors.terminate().map(_ => Done))
  }

  override def forceShutdown: Future[Done] = {
    KafkaConnectors
      .shutdownAll(allControls)
      .flatMap(_ => environment.actors.terminate().map(_ => Done))
  }

  def isShutdown: Boolean = eventHandlerControl.isShutdown.isCompleted
}
