package com.workflowfm.pew.stateless.instances.kafka.components

import akka._
import akka.kafka._
import akka.kafka.scaladsl._
import akka.stream.scaladsl._
import com.workflowfm.pew.stateless.components.StatelessComponent
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorEnvironment

import scala.concurrent.Future

/** Mid-Level Kafka Interface:
  * Defines the akka sources, flows, and sinks, which the high-level interface (KafkaConnectors) builds
  * executable wrappers for the StatelessComponents with.
  */
object KafkaWrapperFlows {

  import Consumer._
  import Subscriptions._
  import com.workflowfm.pew.stateless.StatelessMessages._

  type Environment = KafkaExecutorEnvironment

  /// KAFKA CONSUMER / AKKA SOURCES ///

  /** Kafka Consumer for the `ReduceRequest` topic.
    *
    * @return Akka source containing messages published to the `ReduceRequest` topic.
    */
  def srcReduceRequest[T[X] <: Tracked[X]](
      implicit env: Environment,
      f: TrackedSource[T]
  ): Source[T[ReduceRequest], Control] =
    Tracked.source(env.csReduceRequest, topics(env.settings.tnReduceRequest))

  /** Kafka Consumer for the `Assignment` topic.
    *
    * @return Akka source containing messages published to the `Assignment` topic.
    */
  def srcAssignment[T[X] <: Tracked[X]](
      implicit env: Environment,
      f: TrackedSource[T]
  ): Source[T[Assignment], Control] =
    Tracked.source(env.csAssignment, topics(env.settings.tnAssignment))

  /** Synchronous handling intended for consuming a single `PiiHistory` partition. Blocks until a ReduceRequest is
    * released by receiving both a PiiUpdate *and* at least one SequenceRequest. All SequenceRequests received are
    * responded to, even if this requires sending a ReduceRequest for a PiiUpdate that contains no SequenceRequests.
    *
    * @return Akka flow capable of sequencing a single `PiiHistory` partition into a ReduceRequest stream.
    */
  def flowSequencer[T[X] <: Tracked[X]]: Flow[T[PiiHistory], T[Seq[AnyMsg]], NotUsed] =
    Flow[T[PiiHistory]]
      .scan(new SequenceResponseBuilder[T])(_ next _)
      .map(_.response)
      .collect({ case Some(m) => m })

  /** Kafka Consumer for the `PiiHistory` topic. Exposes the individual partition sources so they handled individually
    * by `flowPartition` argument.
    *
    * @return Akka source containing the processed output messages of each partition.
    */
  def srcPiiHistory[T[X] <: Tracked[X]](
      implicit env: Environment,
      f: TrackedSource[T]
  ): Source[T[PiiHistory], Control] =
    Tracked.source(env.csPiiHistory, topics(env.settings.tnPiiHistory))

  /** Kafka Consumer for the `Result` topic. Configurable Group Id to allow control of the starting point
    * of consumption.
    *
    * @param groupId GroupId for the Kafka Consumer.
    * @return Akka source containing messages published to the `Results` topic.
    */
  def srcResult[T[X] <: Tracked[X]](
      groupId: String
  )(implicit env: Environment, f: TrackedSource[T]): Source[T[PiiLog], Control] =
    Tracked.source(env.csResult withGroupId groupId, topics(env.settings.tnResult))

  /** Kafka Consumers for each topic merged into a single Akka source.
    *
    * @return Merged source for all topics.
    */
  def srcAll(implicit env: Environment): Source[CommitTracked[AnyMsg], Control] = {

    def src[V <: AnyMsg](
        cs: ConsumerSettings[_, V],
        topic: String
    ): Source[CommitTracked[AnyMsg], Control] = CommitTracked
      .source(cs, topics(topic))
      .map(Tracked.fmap(_.asInstanceOf[AnyMsg]))

    Seq(
      src(env.csReduceRequest, env.settings.tnReduceRequest),
      src(env.csPiiHistory, env.settings.tnPiiHistory),
      src(env.csAssignment, env.settings.tnAssignment),
      src(env.csResult, env.settings.tnResult)
    ).reduce(_ merge _)
  }

  /// STREAM PROCESSING / AKKA FLOWS ///

  def flowLogIn[T <: Tracked[_]](implicit env: Environment): Flow[T, T, NotUsed] =
    Flow[T].wireTap(m => env.settings.logMessageReceived(m))

  def flowLogOut[T <: Tracked[_]](implicit env: Environment): Flow[T, T, NotUsed] =
    Flow[T].wireTap(m => env.settings.logMessageSent(m))

  def flowRespond[T[X] <: Tracked[X], In, Out](
      component: StatelessComponent[In, Out]
  ): Flow[T[In], T[Out], NotUsed] = Flow[T[In]]
    .map(Tracked.fmap[T, In, Out](component.respond))

  def flowRespondAll[T[X] <: Tracked[X], In, Out](
      component: StatelessComponent[In, Out]
  ): Flow[T[Seq[In]], T[Seq[Out]], NotUsed] = Flow[T[Seq[In]]]
    .map(Tracked.fmap[T, Seq[In], Seq[Out]](_ map component.respond))

  def flowWaitFuture[T[X] <: Tracked[X], Msg](
      parallelism: Int
  )(implicit env: Environment): Flow[T[Future[Msg]], T[Msg], NotUsed] = {

    Flow[T[Future[Msg]]].mapAsync(parallelism)(Tracked.exposeFuture(_)(env.context))
  }

  def flowCheck[T]: Flow[Tracked[T], T, NotUsed] = Flow[Tracked[T]]
    .map(_.value)

  def flowCheckMulti[T]: Flow[Tracked[Seq[T]], T, NotUsed] = Flow[Tracked[Seq[T]]]
    .map(_.value)
    .flatMapConcat(s => Source.fromIterator(() => s.iterator))

  /// KAFKA PRODUCER / AKKA SINKS ///

  def sinkPlain(implicit env: Environment): Sink[AnyMsg, Future[Done]] = Producer
    .plainSink(env.psAllMessages)
    .contramap(env.settings.record)

  /// OTHER FUNCTIONALITY ///

  def run[T](source: Source[T, Control], sink: Sink[T, Future[Done]])(
      implicit env: Environment
  ): DrainingControl[Done] = source
    .toMat(sink)(Keep.both)
    .mapMaterializedValue(DrainingControl.apply) // Add shutdown control object.
    .named(this.getClass.getSimpleName) // Name for debugging.
    .run()(env.materializer)

}
