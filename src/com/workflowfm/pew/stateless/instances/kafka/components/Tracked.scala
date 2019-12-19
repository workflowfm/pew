package com.workflowfm.pew.stateless.instances.kafka.components

import akka.kafka.ConsumerMessage.{Committable, CommittableOffset, CommittableOffsetBatch, PartitionOffset}
import akka.kafka._
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer, Transactional}
import akka.stream.scaladsl.{Sink, Source}
import akka.{Done, NotUsed}
import com.workflowfm.pew.stateless.StatelessMessages.AnyMsg
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorEnvironment
import com.workflowfm.pew.util.ClassLoaderUtil.withClassLoader
import org.bson.types.ObjectId

import scala.concurrent.{ExecutionContext, Future}

/** Superclass wrapper around objects processed from objects in Kafka topics. The wrapper
  * contains the information necessary to update to consume the input messages when the
  * Tracked wrapper is eventually "Produced" to an "output" Kafka topic.
  *
  * @tparam Value The type of the object contained by the tracking wrapper.
  */
abstract class Tracked[Value] {
  def value: Value

  /** Modify the wrapped `Value` by the function `fn` and preserve the tracking information.
    *
    * @param fn A function to apply to the wrapped `Value`.
    * @tparam NewValue The result type of the function.
    * @return A new Tracked type with the same tracking, containing the new value.
    */
  protected def map[NewValue]( fn: Value => NewValue ): Tracked[NewValue]

  /** Apply a fold to 2 messages so that their tracking information can be combined.
    *
    * @param fn 2 argument fold function to apply to the wrapped types.
    * @param other The other tracked type to merge.
    * @tparam NewValue The type of the wrapped object of the other tracked object.
    * @return A new Tracked object with the result of `fn` on each wrapped object
    *         which contains the merged tracking information.
    */
  protected def fold[NewValue]( fn: (Value, NewValue) => Value )( other: Tracked[NewValue] ): Tracked[Value]
}

/** Superclass of objects capable of creating new Kafka Sources of Tracked types.
  *
  * @tparam T The tracked wrapper type of the objects emitted by the Source.
  */
trait TrackedSource[ T[X] <: Tracked[X] ] {
  implicit val trackedSource: TrackedSource[T] = this
  def source[V]( cs: ConsumerSettings[_, V], sub: AutoSubscription ): Source[T[V], Control]
}

/** Superclass of factory objects capable of creating new Kafka Sinks for Tracked types.
  *
  * @tparam T The tracked wrapper type of the objects consumed by the Sink.
  */
trait TrackedSink[ T[X] <: Tracked[X] ] {
  implicit val trackedSink: TrackedSink[T] = this
  def sink[V <: AnyMsg]( implicit s: KafkaExecutorEnvironment ): Sink[T[V], Future[Done]]
}

/** Superclass of factory objects capable of creating new Kafka Sinks for Tracked collection types.
  *
  * @tparam T The tracked wrapper type of the objects consumed by the Sink.
  */
trait TrackedMultiSink[ T[X] <: Tracked[X] ] {
  implicit val trackedMultiSink: TrackedMultiSink[T] = this
  def sinkMulti[V <: AnyMsg]( implicit s: KafkaExecutorEnvironment ): Sink[T[Seq[V]], Future[Done]]
}

/** Tracked type helper functions, using implicits to redirect to the correct
  * `TrackedSource`s, `TrackedSink`s, and `TrackedMultiSink`s.
  */
object Tracked {

  def source[T[X] <: Tracked[X], V]
    ( cs: ConsumerSettings[_, V], sub: AutoSubscription )
    ( implicit s: KafkaExecutorEnvironment, ts: TrackedSource[T] )
    : Source[T[V], Control] = ts.source( cs, sub )

  def sink[T[X] <: Tracked[X], V <: AnyMsg]( implicit s: KafkaExecutorEnvironment, ts: TrackedSink[T] ): Sink[T[V], Future[Done]]
    = ts.sink( s )

  def sinkMulti[T[X] <: Tracked[X], V <: AnyMsg]( implicit s: KafkaExecutorEnvironment, ts: TrackedMultiSink[T] ): Sink[T[Seq[V]], Future[Done]]
    = ts.sinkMulti( s )

  /** Map a function over the value within a tracked type.
    *
    * @param fn Function apply to input wrapped value.
    * @param trackedIn Tracked type containing input value.
    * @tparam T Tracked type of both input and output wrappers.
    * @tparam In Wrapped input type.
    * @tparam Out Wrapped output type.
    * @return A new Tracked type (of type `T`) with value of the output of `fn`.
    */
  def fmap[T[X] <: Tracked[X], In, Out]( fn: In => Out )( trackedIn: T[In] ): T[Out]
    = trackedIn.map( fn ).asInstanceOf[T[Out]]

  /** Replace the value within a Tracked wrapper without updating the tracking information.
    *
    * @param tracked The Tracked wrapper to modify.
    * @param newValue The new value for the Tracked wrapper.
    * @tparam T Tracked type of both input and output wrappers.
    * @tparam In Wrapped input type.
    * @tparam Out Wrapped output type.
    * @return A new Tracked object of type `T`, containing `newValue` and the tracking from `tracked`.
    */
  def freplace[T[X] <: Tracked[X], In, Out]( tracked: T[In] )( newValue: Out ): T[Out]
    = fmap( (_: In) => newValue )( tracked )

  /** Expose a future contained within a Tracked wrapper,
    *
    * @param fut Tracked wrapper containing a future.
    * @param exec Execution context of the future.
    * @tparam V Type of the future.
    * @tparam T Tracked type of both input and output wrappers.
    * @return A future returning a Tracked type of the result of the original future.
    */
  def exposeFuture[V, T[X] <: Tracked[X]]( fut: T[Future[V]] )( implicit exec: ExecutionContext ): Future[T[V]]
    = fut.value.map( freplace( fut ) )( exec )

  /** Merge the tracking information of a sequence of Tracked wrappers.
    * NOTE: CONDITIONS APPLY! CHECK THE TRACKED CLASSES FOLD FUNCTION.
    *
    * @param consuming A sequence of messages to combine.
    * @tparam Msg The type of the elements in the sequence.
    * @tparam T Tracked type of both input and output wrappers.
    * @return Tracked wrapper of type `T` containing a list of `Msg` objects.
    */
  def flatten[Msg, T[X] <: Tracked[X]]( consuming: Seq[T[Msg]] ): T[Seq[Msg]] = {
    require( consuming.nonEmpty )

    val foldStart: T[Seq[Msg]] = fmap[T, Msg, Seq[Msg]]( Seq(_) )( consuming.head )

    def combine( tseq: T[Seq[Msg]], tmsg: T[Msg] ): T[Seq[Msg]]
      = tseq.fold[Msg]( _ :+ _ )( tmsg ).asInstanceOf[ T[Seq[Msg]] ]

    consuming.tail.foldLeft[T[Seq[Msg]]]( foldStart )( combine )
  }

  /** Create a KafkaProducer from settings whilst explicitly un-setting the ClassLoader.
    * This by-passes errors encountered in the KafkaProducer constructor where Threads
    * within an ExecutionContext do not list the necessary key or value serialiser classes.
    * Explicitly setting `null` causes the constructor to use the Kafka ClassLoader
    * which should contain these values.
    *
    * (Note: Use `lazyProducer` to minimize the number of new Producers which are created,
    * this reduces the number of system resources used (such as file handles))
    * Note on the note: lazyProducer is no longer available in the latest version
    */
  def createProducer[K, V]( settings: ProducerSettings[K, V] ): org.apache.kafka.clients.producer.Producer[K, V]
    = withClassLoader( null ) { settings.createKafkaProducer() }
}

/** A Tracked type which uses a `Committable` as tracking information.
  *
  * @tparam Value The type of the object contained by the tracking wrapper.
  */
trait HasCommittable[Value] extends Tracked[Value] {
  def commit: Committable
}

/** A factory object creating Sinks for `HasCommittable` types.
  *
  * @tparam T The tracked wrapper type of the objects consumed by the Sink.
  */
trait HasCommittableSinks[T[X] <: HasCommittable[X]]
  extends TrackedSink[T]
  with TrackedMultiSink[T] {

  override def sink[V <: AnyMsg]( implicit s: KafkaExecutorEnvironment ): Sink[HasCommittable[V], Future[Done]]
    = Producer.commitableSink( s.psAllMessages, Tracked.createProducer( s.psAllMessages ) )
      .contramap( msg =>
        ProducerMessage.Message(
          s.settings.record( msg.value ),
          msg.commit
        )
      )

  override def sinkMulti[V <: AnyMsg]( implicit s: KafkaExecutorEnvironment ): Sink[HasCommittable[Seq[V]], Future[Done]]
    = Producer.commitableSink( s.psAllMessages, Tracked.createProducer( s.psAllMessages ) )
      .contramap( msgs =>
        ProducerMessage.MultiMessage(
          msgs.value.map(s.settings.record).to,
          msgs.commit
        )
      )
}

/** A Tracked wrapper using a `Committable` with an unknown source partition.
  *
  * @param value The wrapped object.
  * @param commit Commit information of the source message.
  * @tparam Value The type of the object contained by the tracking wrapper.
  */
case class CommitTracked[Value](
    value: Value,
    commit: Committable
  ) extends Tracked[Value] with HasCommittable[Value] {

  override protected def map[NewValue](fn: Value => NewValue): Tracked[NewValue]
    = copy( value = fn( value ) )

  override protected def fold[NewValue](fn: (Value, NewValue) => Value)(other: Tracked[NewValue]): Tracked[Value]
    = copy(
      value = fn( value, other.value ),
      commit = CommitTracked.unsafeMerge( commit, other.asInstanceOf[CommitTracked[_]].commit )
    )
}

object CommitTracked
  extends TrackedSource[CommitTracked]
  with HasCommittableSinks[CommitTracked] {

  def unsafeMerge( left: Committable, right: Committable ): Committable = {
    require( right.isInstanceOf[CommittableOffset] )

    ( left match {
      case offset: CommittableOffset => ConsumerMessage.emptyCommittableOffsetBatch.updated( offset )
      case batch: CommittableOffsetBatch => batch
    }).updated( right.asInstanceOf[CommittableOffset] )
  }

  def source[Value]( cs: ConsumerSettings[_, Value], sub: AutoSubscription ): Source[CommitTracked[Value], Control]
    = Consumer.committableSource( cs, sub )
      .map( msg =>
        CommitTracked(
          msg.record.value,
          msg.committableOffset
        )
      )
}

/** A Tracked wrapper superclass for Tracked types which know the partition of their inputs.
  * NOTE: THESE OBJECTS CANNOT BE FLATTENED FROM SEPARATE PARTITIONS!
  *
  * @tparam Value The type of the object contained by the tracking wrapper.
  */
trait HasPartition[Value]
  extends Tracked[Value] {

  /** Partition of the source message of this object.
    *
    * @return The partition ID of the source message.
    */
  def part: Int
}

/** A Tracked wrapper using a `Committable` with an known source partition.
  *
  * @param value The wrapped object.
  * @param commit Commit information of the source message.
  * @param part The partition of the source message.
  * @tparam Value The type of the object contained by the tracking wrapper.
  */
case class PartTracked[Value](
   value: Value,
   commit: Committable,
   part: Int

  ) extends Tracked[Value]
  with HasCommittable[Value]
  with HasPartition[Value] {

  override protected def map[NewValue](fn: Value => NewValue): Tracked[NewValue]
    = copy( value = fn( value ) )

  override protected def fold[NewValue](fn: (Value, NewValue) => Value)(other: Tracked[NewValue]): Tracked[Value]
    = copy(
      value = fn( value, other.value ),
      commit = CommitTracked.unsafeMerge( commit, other.asInstanceOf[PartTracked[_]].commit )
    )
}

object PartTracked
  extends TrackedSource[PartTracked]
  with HasCommittableSinks[PartTracked] {

  def source[Value]( cs: ConsumerSettings[_, Value], sub: AutoSubscription ): Source[PartTracked[Value], Control]
    = Consumer.committablePartitionedSource( cs, sub )
      .flatMapMerge( Int.MaxValue, {
        case (topicPartition, source) =>
          println(s"Started consuming partition $topicPartition.")
          source.map( msg =>
            PartTracked(
              msg.record.value,
              msg.committableOffset,
              topicPartition.partition()
            )
          )
      })

}

/** A Tracked wrapper type using the `Transactional` Kafka interface. `Transactional` commits
  * ensure the topic offset updates and message production happen atomicly.
  *
  * This is necessary for exactly-once message semantics.
  *
  * @param value The wrapped object.
  * @param partOffset Transaction information.
  * @tparam Value The type of the object contained by the tracking wrapper.
  */
case class Transaction[Value](
    value: Value,
    partOffset: PartitionOffset

  ) extends Tracked[Value] with HasPartition[Value] {

  def offset: Long = partOffset.offset

  override def part: Int = partOffset.key.partition

  override protected def map[NewValue](fn: Value => NewValue): Tracked[NewValue]
    = copy( value = fn( value ) )

  override protected def fold[NewValue](fn: (Value, NewValue) => Value)(other: Tracked[NewValue]): Tracked[Value] = {
    assert( assertion = false, "This function should work, but causes hangs when used with Transactional.sink" )

    require( other.isInstanceOf[Transaction[NewValue]] )
    val that = other.asInstanceOf[Transaction[NewValue]]

    require( partOffset.key == that.partOffset.key, "Trying to merge offsets of different partitions." )
    require( partOffset.offset + 1 == that.partOffset.offset, "Skipping Consumer message." )
    copy(
      value = fn( value, that.value ),
      partOffset = partOffset.copy( offset = Math.max( offset, that.offset ) )
    )
  }
}

object Transaction
  extends TrackedSource[Transaction]
  with TrackedSink[Transaction]
  with TrackedMultiSink[Transaction] {

  implicit val meh: TrackedSource[Transaction] = this
  implicit val meh2: TrackedSink[Transaction] = this
  implicit val meh3: TrackedMultiSink[Transaction] = this

  override def source[Value]( cs: ConsumerSettings[_, Value], sub: AutoSubscription ): Source[Transaction[Value], Control]
    = Transactional.source( cs, sub )
      .map( msg => Transaction( msg.record.value(), msg.partitionOffset ) )

  override def sink[V <: AnyMsg]( implicit s: KafkaExecutorEnvironment ): Sink[Transaction[V], Future[Done]] = {
    // TODO: Construct the KafkaProducer with the correct ClassLoader
    val id = ObjectId.get.toString
    Transactional.sink( s.psAllMessages, id )
    .contramap( msg =>
      ProducerMessage.Message(
        s.settings.record( msg.value ),
        msg.partOffset
      )
    )
  }

  override def sinkMulti[V <: AnyMsg]( implicit s: KafkaExecutorEnvironment ): Sink[Transaction[Seq[V]], Future[Done]] = {
    // TODO: Construct the KafkaProducer with the correct ClassLoader
    val id = ObjectId.get.toString
    Transactional.sink( s.psAllMessages, id )
    .contramap( msgs =>
      ProducerMessage.MultiMessage(
        msgs.value.map(s.settings.record).to,
        msgs.partOffset
      )
    )
  }
}

/** Untracked objects which satisfy this interface, messages are consumed immediately at their Source.
  *
  * @param value The wrapped object.
  * @tparam Value The type of the object contained by the tracking wrapper.
  */
case class Untracked[Value](
   value: Value
 ) extends Tracked[Value] {

  override protected def map[NewValue](fn: Value => NewValue): Tracked[NewValue]
    = copy( value = fn( value ) )

  override protected def fold[NewValue](fn: (Value, NewValue) => Value)(other: Tracked[NewValue]): Tracked[Value]
    = copy( value = fn( value, other.value ) )
}

object Untracked
  extends TrackedSource[Untracked]
  with TrackedSink[Untracked] {

  def source[Value]( messages: Seq[Value] ): Source[Untracked[Value], NotUsed]
    = Source.fromIterator( () => messages.iterator ).map( Untracked(_) )

  override def source[Value]( cs: ConsumerSettings[_, Value], sub: AutoSubscription ): Source[Untracked[Value], Control]
    = Consumer.committableSource( cs, sub )
      .mapAsync(1)( message =>
        message
        .committableOffset
        .commitScaladsl()
        .map( _ => Untracked( message.record.value ) )( ExecutionContext.global )
      )

  override def sink[Value <: AnyMsg]( implicit s: KafkaExecutorEnvironment ): Sink[Untracked[Value], Future[Done]]
    = Producer.plainSink( s.psAllMessages, Tracked.createProducer( s.psAllMessages ) )
      .contramap( (msg: Untracked[Value]) => s.settings.record( msg.value ) )

}

/** Mock Tracked wrapper for testing, matches the requirements of the Transactional trackers.
  */
case class MockTransaction[Value](
    value: Value,
    part: Int,
    minOffset: Long,
    maxOffset: Long

  ) extends Tracked[Value] with HasPartition[Value] {

  def this( value: Value, part: Int, offset: Long )
    = this( value, part, offset, offset + 1 )

  def consuming: Long = maxOffset - minOffset

  override protected def map[NewValue](fn: Value => NewValue): Tracked[NewValue]
    = copy( value = fn( value ) )

  override protected def fold[NewValue](fn: (Value, NewValue) => Value)(other: Tracked[NewValue]): Tracked[Value] = {
    require( other.isInstanceOf[MockTransaction[NewValue]] )
    val that: MockTransaction[NewValue] = other.asInstanceOf[MockTransaction[NewValue]]

    // Strict checks to emulate requirements merging requirements of real Transactions.
    require( part == that.part, s"Cannot merge messages on different partitions" )
    require( that.minOffset + 1 == that.maxOffset, s"Target message cannot already be aggregated!" )
    require( minOffset < maxOffset, s"Messages must be consumed in chronological order!" )
    require( that.minOffset < that.maxOffset, s"Messages must be consumed in chronological order!" )
    require( that.minOffset == maxOffset, s"Messages cannot be skipped! ($maxOffset -> ${that.minOffset})" )

    copy( value = fn( value, that.value ), maxOffset = that.maxOffset )
  }
}

object MockTransaction {

  def source[Value]( messages: Seq[(Value, Int)] ): Source[MockTransaction[Value], NotUsed]
    = Source
      .fromIterator( () => messages.iterator )
      .groupBy( Int.MaxValue, _._2 )
      .zip( Source(1 to Int.MaxValue) )
      .map({ case ((msg, part), i) => new MockTransaction( msg, part, i ) })
      .mergeSubstreams

}

/** Mock Tracked wrapper for testing, matches the requirements of the Commitable trackers.
  */
case class MockTracked[Value](
    value: Value,
    part: Int,
    consuming: Long

  ) extends Tracked[Value] {

  override protected def map[NewValue](fn: Value => NewValue): Tracked[NewValue]
    = copy( value = fn( value ) )

  override protected def fold[NewValue](fn: (Value, NewValue) => Value)(other: Tracked[NewValue]): Tracked[Value] = {
    require( other.isInstanceOf[MockTracked[NewValue]] )
    val that: MockTracked[NewValue] = other.asInstanceOf[MockTracked[NewValue]]

    copy( value = fn( value, that.value ), consuming = consuming + that.consuming )
  }
}

object MockTracked {

  def apply[T]( mock: MockTransaction[T] ): MockTracked[T]
    = MockTracked( mock.value, mock.part, mock.maxOffset - mock.minOffset )

  def source[Value]( messages: Seq[(Value, Int)] ): Source[MockTracked[Value], NotUsed]
    = Source
      .fromIterator( () => messages.iterator )
      .map({ case (v, p) => MockTracked(v, p, 1) })

}

