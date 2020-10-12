package com.workflowfm.pew.stateless.instances.kafka.components

import scala.concurrent.{ ExecutionContext, Future }

import akka.Done
import akka.kafka.scaladsl.Consumer.{ Control, DrainingControl }
import akka.stream.scaladsl.Sink
import org.bson.types.ObjectId

import com.workflowfm.pew.PiEventFinish
import com.workflowfm.pew.stateless.StatelessMessages
import com.workflowfm.pew.stateless.components._
import com.workflowfm.pew.stateless.instances.kafka.settings.{
  KafkaExecutorEnvironment,
  KafkaExecutorSettings
}

/** High-Level Kafka Interface:
  * Responsible for correctly wrapping StatelessComponents into RunnableGraphs to execute
  * off of a Kafka cluster. Makes use of a KafkaExecutorSettings instance for the correct
  * construction of Kafka Producers and Consumers.
  *
  * To fulfill the Executor interface, the following connectors must run on a kafka cluster:
  *
  * - EITHER: At least one `indyReducer` & `indySequencer`
  *   OR: At least on `seqReducer`
  *
  * - At least one `indyAtomicExecutor`
  *
  * - OPTIONAL: Either a `specificResultListener` or `uniqueResultListener` if the results
  *   of each PiInstance execution need responding to.
  */
object KafkaConnectors {

  import KafkaWrapperFlows._
  import StatelessMessages._

  type DrainControl = DrainingControl[Done]
  type Environment = KafkaExecutorEnvironment
  type Settings = KafkaExecutorSettings

  /** Creates a temporary Kafka Producer capable of sending a single message.
    *
    * @param msgs Messages to send.
    * @param s KafkaExecutorSettings controlling the interface with the Kafka Driver.
    * @return
    */
  def sendMessages(msgs: AnyMsg*)(implicit env: Environment): Future[Done] = Untracked
    .source(msgs)
    .via(flowLogOut)
    .runWith(Untracked.sink)(env.materializer)

  /** Run an independent reducer off of a ReduceRequest topic. This allows a
    * reducer to create responses to each ReduceRequest individually by using the
    * ReduceRequest topic offset to track each messages consumption.
    *
    * @param red Reducer module responsible for generating responses for ReduceRequests.
    * @param s KafkaExecutorSettings controlling the interface with the Kafka Driver.
    * @return Control object for the running process.
    */
  def indyReducer(red: Reducer)(implicit env: Environment): DrainControl = run(
    srcReduceRequest[PartTracked]
      via flowLogIn
      via flowRespond(red)
      via flowLogOut,
    Tracked.sinkMulti[PartTracked, AnyMsg]
  )

  /** Run an independent sequencer off of a PiiHistory topic, outputting sequenced
    * ReduceRequests into a separate ReduceRequest topic.
    *
    * @param s KafkaExecutorSettings controlling the interface with the Kafka Driver.
    * @return Control object for the running process.
    */
  def indySequencer(implicit env: Environment): DrainControl = run(
    srcPiiHistory[PartTracked]
      via flowLogIn
      groupBy (Int.MaxValue, _.part)
      via flowSequencer
      via flowLogOut mergeSubstreams,
    Tracked.sinkMulti[PartTracked, AnyMsg]
  )

  /** Run a reducer directly off the output of a Sequencer. Doing this negates the need
    * for a ReduceRequest topic, however it does necessitate the consumption of multiple
    * ReduceRequests simultaneously if they have been tangled in the PiiHistory.
    *
    * @param red Reducer component responsible for generating responses for ReduceRequests.
    * @param s KafkaExecutorSettings controlling the interface with the Kafka Driver.
    * @return Control object for the running process.
    */
  /* TODO: Reimplement when we can do many-to-many transactions.
   * def seqReducer( reducer: Reducer )( implicit s: Settings ): Control
   * = run[Seq[AnyMsg]]( srcPiiHistory groupBy( Int.MaxValue, _.part ) via flowSequencer map (
   * _.map( _.collect({ case m: ReduceRequest => reducer respond m }).flatten ) ) mergeSubstreams,
   * sinkTransactionalMulti( "SeqReducer" ) ) */

  /** Run a AtomicExecutor off of the Assignment topic.
    *
    * @param exec AtomicExecutor component responsible for evaluating assignments and responding with SequenceRequests.
    * @param threadsPerPart Parallelism of each Consumer topic partition.
    * @param s KafkaExecutorSettings controlling the interface with the Kafka Driver.
    * @return Control object for the running process.
    */
  def indyAtomicExecutor(exec: AtomicExecutor, threadsPerPart: Int = 1)(
      implicit env: Environment
  ): DrainControl = run(
    srcAssignment[PartTracked]
      via flowLogIn
      groupBy (Int.MaxValue, _.part)
      via flowRespond(exec)
      via flowWaitFuture(threadsPerPart)
      via flowLogOut mergeSubstreams,
    Tracked.sinkMulti[PartTracked, AnyMsg]
  )

  /** Restart a terminated ResultListener group, join an existing group, or start a ResultListener with a specific
    * group id. Useful as PiiResult messages might need to be visible to multiple ResultListeners.
    *
    * @param s KafkaExecutorSettings controlling the interface with the Kafka Driver.
    * @return Control object for the running process.
    */
  def specificResultListener(groupId: String)(resl: ResultListener)(
      implicit env: Environment
  ): DrainControl = run(
    srcResult[Untracked](groupId)
      wireTap { msg =>
        msg.value.event match {
          case res: PiEventFinish[_] =>
            env.settings.logMessageReceived(res)

          case _ => // Other messages are uninteresting.
        }
      }
      via flowRespond(resl),
    Sink.ignore
  )

  /** Override `source` to give handler a uniqueGroupId so it each KafkaEventHandler
    * component can listen to all events on the cluster. The ResultListener is responsible
    * for ignoring the irrelevant messages appropriately.
    *
    * @param s KafkaExecutorSettings controlling the interface with the Kafka Driver.
    * @return Control object for the running process.
    */
  def uniqueResultListener(resl: ResultListener)(implicit env: Environment): DrainControl =
    specificResultListener("Event-Group-" + ObjectId.get.toHexString)(resl)

  def shutdown(controls: Control*)(implicit env: Environment): Future[Done] = shutdownAll(controls)

  def shutdownAll(controls: Seq[Control])(implicit env: Environment): Future[Done] = {
    implicit val ctx: ExecutionContext = env.context
    Future.sequence(controls map (_.shutdown())).map(_ => Done)
  }

  def drainAndShutdown(controls: DrainControl*)(implicit env: Environment): Future[Done] =
    drainAndShutdownAll(controls)

  def drainAndShutdownAll(controls: Seq[DrainControl])(implicit env: Environment): Future[Done] = {
    implicit val ctx: ExecutionContext = env.context
    Future.sequence(controls map (_.drainAndShutdown())).map(_ => Done)
  }
}
