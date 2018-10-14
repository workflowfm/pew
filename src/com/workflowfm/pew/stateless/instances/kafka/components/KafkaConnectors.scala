package com.workflowfm.pew.stateless.instances.kafka.components

import akka.Done
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.scaladsl.Sink
import com.workflowfm.pew.stateless.StatelessMessages
import com.workflowfm.pew.stateless.components._
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings
import org.bson.types.ObjectId

import scala.concurrent.{ExecutionContext, Future}

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

  /** Creates a temporary Kafka Producer capable of sending a single message.
    *
    * @param msgs Messages to send.
    * @param s KafkaExecutorSettings controlling the interface with the Kafka Driver.
    * @return
    */
  def sendMessages( msgs: AnyMsg* )( implicit s: KafkaExecutorSettings ): Future[Done]
    = Untracked.source( msgs )
      .via( flowLogOut )
      .runWith( Untracked.sink )( s.mat )

  /** Run an independent reducer off of a ReduceRequest topic. This allows a
    * reducer to create responses to each ReduceRequest individually by using the
    * ReduceRequest topic offset to track each messages consumption.
    *
    * @param red Reducer module responsible for generating responses for ReduceRequests.
    * @param s KafkaExecutorSettings controlling the interface with the Kafka Driver.
    * @return Control object for the running process.
    */
  def indyReducer( red: Reducer )(implicit s: KafkaExecutorSettings ): Control
    = run(
      srcReduceRequest[PartTracked]
      via flowLogIn
      via flowRespond( red )
      via flowLogOut,
      Tracked.sinkMulti[PartTracked, AnyMsg]
    )

  /** Run an independent sequencer off of a PiiHistory topic, outputting sequenced
    * ReduceRequests into a separate ReduceRequest topic.
    *
    * @param s KafkaExecutorSettings controlling the interface with the Kafka Driver.
    * @return Control object for the running process.
    */
  def indySequencer( implicit s: KafkaExecutorSettings ): Control
    = run(
      srcPiiHistory[PartTracked]
      via flowLogIn
      groupBy( Int.MaxValue, _.part )
      via flowSequencer
      via flowLogOut
      mergeSubstreams,
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
    def seqReducer( reducer: Reducer )(implicit s: KafkaExecutorSettings ): Control
      = run[Seq[AnyMsg]](
        srcPiiHistory
        groupBy( Int.MaxValue, _.part )
        via flowSequencer
        map ( _.map( _.collect({ case m: ReduceRequest => reducer respond m }).flatten ) )
        mergeSubstreams,
        sinkTransactionalMulti( "SeqReducer" )
    )*/

  /** Run a AtomicExecutor off of the Assignment topic.
    *
    * @param exec AtomicExecutor component responsible for evaluating assignments and responding with SequenceRequests.
    * @param threadsPerPart Parallelism of each Consumer topic partition.
    * @param s KafkaExecutorSettings controlling the interface with the Kafka Driver.
    * @return Control object for the running process.
    */
  def indyAtomicExecutor( exec: AtomicExecutor, threadsPerPart: Int = 1 )( implicit s: KafkaExecutorSettings ): Control
    = run(
      srcAssignment[PartTracked]
      via flowLogIn
      groupBy( Int.MaxValue, _.part )
      via flowRespond( exec )
      via flowWaitFuture( threadsPerPart )
      via flowLogOut
      mergeSubstreams,
      Tracked.sink[PartTracked, AnyMsg]
    )

  /** Restart a terminated ResultListener group, join an existing group, or start a ResultListener with a specific
    * group id. Useful as PiiResult messages might need to be visible to multiple ResultListeners.
    *
    * @param s KafkaExecutorSettings controlling the interface with the Kafka Driver.
    * @return Control object for the running process.
    */
  def specificResultListener( groupId: String )( resl: ResultListener )(implicit s: KafkaExecutorSettings ): Control
    = run(
      srcResult[Untracked]( groupId )
      via flowRespond( resl ),
      Sink.ignore
    )

  /** Override `source` to give handler a uniqueGroupId so it each KafkaEventHandler
    * component can listen to all events on the cluster. The ResultListener is responsible
    * for ignoring the irrelevant messages appropriately.
    *
    * @param s KafkaExecutorSettings controlling the interface with the Kafka Driver.
    * @return Control object for the running process.
    */
  def uniqueResultListener( resl: ResultListener )( implicit s: KafkaExecutorSettings ): Control
    = specificResultListener( "Event-Group-" + ObjectId.get.toHexString )( resl )( s )

  def shutdown( controls: Control* )( implicit s: KafkaExecutorSettings ): Future[Done] = shutdownAll( controls )

  def shutdownAll( controls: Seq[Control] )( implicit s: KafkaExecutorSettings ): Future[Done] = {
    implicit val ctx: ExecutionContext = s.execCtx
    Future.sequence( controls.map( c => c.shutdown().flatMap( _ => c.isShutdown ) ) ).map( _ => Done )
  }
}
