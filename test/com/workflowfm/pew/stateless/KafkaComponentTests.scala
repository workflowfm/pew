package com.workflowfm.pew.stateless

import akka.kafka.ConsumerMessage.{GroupTopicPartition, PartitionOffset}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.stateless.components.AtomicExecutor
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaWrapperFlows._
import com.workflowfm.pew.{PiInstance, PiItem, PiObject}
import org.bson.types.ObjectId
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class KafkaComponentTests extends FlatSpec with Matchers with BeforeAndAfterAll with KafkaTests {

  def fixtureSequenceRequest = new {
    val p1 = PiInstance( ObjectId.get, pbi, PiObject(1) )
    val p2 = PiInstance( ObjectId.get, pbi, PiObject(1) )

    def offset( part: Int ): PartitionOffset
      = PartitionOffset( GroupTopicPartition( "test", "test", part ), 1 )

    def runSequencer( history: Tracked[PiiHistory]* ): Seq[Tracked[Seq[AnyMsg]]]
      = await(
        Source
          .fromIterator( () => history.iterator )
          .groupBy( Int.MaxValue, _.part )
          .zip( Source(1 to 2000) )
          .map({
            case (t, i) =>
              t.copy( partOffset = t.partOffset.copy( offset = i ) )
          })
          .via( flowSequencer )
          .mergeSubstreams
          .runWith( Sink.seq )( ActorMaterializer() )
      )

    val result = (CallRef(0), PiObject(0))

    def update( pii: PiInstance[ObjectId], part: Int ): Tracked[PiiHistory]
    = Tracked( PiiUpdate( pii ), offset( part ) )

    def seqreq( pii: PiInstance[ObjectId], part: Int ): Tracked[PiiHistory]
    = Tracked( SequenceRequest( pii.id, result ), offset( part ) )

  }

  it should "sequence 1 PiiUpdate and 1 SequenceRequest" in {
    val f = fixtureSequenceRequest
    val res = f.runSequencer(
      f.update(f.p1, 1),
      f.seqreq(f.p1, 1)
    )

    res should have size 1
    res.head.value should have size 1
    res.head.offset shouldBe 2
  }

  it should "sequence 2 PiiUpdates from different Piis and 1 SequenceRequest" in {
    val f = fixtureSequenceRequest
    val res = f.runSequencer(
      f.update(f.p1, 1),
      f.update(f.p2, 1),
      f.seqreq(f.p1, 1)
    )

    res should (have size 1)
    res.head.value should (have size 2)
    res.head.offset shouldBe 3
  }

  it should "sequence 4 PiiUpdate of 2 Piis and 1 SequenceRequest" in {
    val f = fixtureSequenceRequest
    val res = f.runSequencer(
      f.update(f.p1, 1),
      f.update(f.p2, 1),
      f.update(f.p1, 1),
      f.update(f.p2, 1),
      f.seqreq(f.p1, 1),

    )

    res should (have size 1)
    res.head.value should (have size 2)
    res.head.offset shouldBe 5
  }

  it should "sequence only 1 of 2 PiiUpdates on different partitions with 1 SequenceRequest" in {
    val f = fixtureSequenceRequest
    val res = f.runSequencer(
      f.update(f.p1, 1),
      f.update(f.p2, 2),
      f.seqreq(f.p1, 1),
    )

    res should (have size 1)
    res.head.value should (have size 1)
    res.head.offset shouldBe 2
  }

  it should "not sequence a PiiUpdate and SequenceRequest for different Piis" in {
    val f = fixtureSequenceRequest
    f.runSequencer(
      f.update(f.p1, 1),
      f.seqreq(f.p2, 1),

    ) shouldBe empty
  }

  it should "not sequence a PiiUpdate and SequenceRequest on different partitions" in {
    val f = fixtureSequenceRequest

    f.runSequencer(
      f.update(f.p1, 1),
      f.seqreq(f.p1, 2),

    ) shouldBe empty
  }

  it should "respond to Assignments with a correct SequenceRequest" in {
    val atomExec: AtomicExecutor = AtomicExecutor()

    val (threads, pii)
      = PiInstance( ObjectId.get, pbi, PiObject(1) )
        .reduce.handleThreads( (_, _) => true )

    val t: Int = threads.head

    val task =
      Assignment(
        pii, CallRef(t) , "Pb",
        pii.piFutureOf(t).get.args
      )

    val response = SequenceRequest( pii.id, ( CallRef(t), PiItem("PbISleptFor1s") ) )

    await( atomExec.respond( task ) ) shouldBe response
  }
}