package com.workflowfm.pew.stateless

import com.workflowfm.pew.PiInstance
import com.workflowfm.pew.execution.RexampleTypes._
import com.workflowfm.pew.stateless.StatelessMessages._
import com.workflowfm.pew.stateless.components.Reducer
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConnectors
import org.bson.types.ObjectId
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.concurrent._
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class KafkaExecutorTests extends FlatSpec with Matchers with BeforeAndAfterAll with KafkaTests {

  // Ensure there are no outstanding messages before starting testing.
  isAllTidy()

  it should "execute atomic PbI once" in {

    val ex = makeExecutor(completeProcessStore)
    val f1 = ex.execute( pbi, Seq(1) )

    await( f1 ) should be ("PbISleptFor1s")
    ex.syncShutdown()

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[Assignment] shouldBe empty
    msgsOf[PiiUpdate] shouldBe empty
  }

  it should "execute atomic PbI twice concurrently" in {

    val ex = makeExecutor(completeProcessStore)
    val f1 = ex.execute(pbi,Seq(2))
    val f2 = ex.execute(pbi,Seq(1))

    await(f1) should be ("PbISleptFor2s")
    await(f2) should be ("PbISleptFor1s")
    ex.syncShutdown()

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[Assignment] shouldBe empty
    msgsOf[PiiUpdate] shouldBe empty
  }

  it should "execute Rexample once" in {

    val ex = makeExecutor(completeProcessStore)
    val f1 = ex.execute(ri,Seq(21))

    await(f1) should be (("PbISleptFor2s","PcISleptFor1s"))
    ex.syncShutdown()

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[Assignment] shouldBe empty
    msgsOf[PiiUpdate] shouldBe empty
  }

  it should "execute Rexample once with same timings" in {

    val ex = makeExecutor(completeProcessStore)
    val f1 = ex.execute(ri,Seq(11))

    await(f1) should be (("PbISleptFor1s","PcISleptFor1s"))
    ex.syncShutdown()

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[Assignment] shouldBe empty
    msgsOf[PiiUpdate] shouldBe empty
  }

  it should "execute Rexample twice concurrently" in {

    val ex = makeExecutor(completeProcessStore)
    val f1 = ex.execute(ri,Seq(31))
    val f2 = ex.execute(ri,Seq(12))

    await(f1) should be (("PbISleptFor3s","PcISleptFor1s"))
    await(f2) should be (("PbISleptFor1s","PcISleptFor2s"))
    ex.syncShutdown()

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[Assignment] shouldBe empty
    msgsOf[PiiUpdate] shouldBe empty
  }

  it should "execute Rexample twice with same timings concurrently" in {

    val ex = makeExecutor(completeProcessStore)
    val f1 = ex.execute(ri,Seq(11))
    val f2 = ex.execute(ri,Seq(11))

    await(f1) should be (("PbISleptFor1s","PcISleptFor1s"))
    await(f2) should be (("PbISleptFor1s","PcISleptFor1s"))
    ex.syncShutdown()

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[Assignment] shouldBe empty
    msgsOf[PiiUpdate] shouldBe empty
  }

  it should "execute Rexample thrice concurrently" in {

    val ex = makeExecutor(completeProcessStore)
    val f1 = ex.execute(ri,Seq(11))
    val f2 = ex.execute(ri,Seq(11))
    val f3 = ex.execute(ri,Seq(11))

    await(f1) should be (("PbISleptFor1s","PcISleptFor1s"))
    await(f2) should be (("PbISleptFor1s","PcISleptFor1s"))
    await(f3) should be (("PbISleptFor1s","PcISleptFor1s"))
    ex.syncShutdown()

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[Assignment] shouldBe empty
    msgsOf[PiiUpdate] shouldBe empty
  }

  it should "execute Rexample twice, each with a differnt component" in {

    val ex = makeExecutor( completeProcessStore )
    val f1 = ex.execute(ri,Seq(11))
    val f2 = ex.execute(ri2,Seq(11))

    await(f1) should be (("PbISleptFor1s","PcISleptFor1s"))
    await(f2) should be (("PbISleptFor1s","PcXSleptFor1s"))
    ex.syncShutdown()

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[Assignment] shouldBe empty
    msgsOf[PiiUpdate] shouldBe empty
  }

  it should "handle a failing atomic process" in {

    val ex = makeExecutor( failureProcessStore )
    val f1 = ex.execute( failp, Seq(1) )

    awaitErr( f1 ).map( _.getMessage ) shouldBe Right( "FailP" )
    ex.syncShutdown()

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[Assignment] shouldBe empty
    msgsOf[PiiUpdate] shouldBe empty
  }

  it should "handle a failing composite process" in {

    val ex = makeExecutor( failureProcessStore )
    val f1 = ex.execute( rif, Seq(21) )

    awaitErr( f1 ).map( _.getMessage ) shouldBe Right( "Fail" )
    ex.syncShutdown()

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[Assignment] shouldBe empty
    msgsOf[PiiUpdate] shouldBe empty
  }

  it should "2 separate executors should execute with same timings concurrently" in {

    val ex1 = makeExecutor(completeProcessStore)
    val ex2 = makeExecutor(completeProcessStore)

    val f1 = ex1.execute(ri,Seq(11))
    val f2 = ex2.execute(ri,Seq(11))

    await(f1) should be (("PbISleptFor1s","PcISleptFor1s"))
    await(f2) should be (("PbISleptFor1s","PcISleptFor1s"))

    ex1.syncShutdown()
    ex2.syncShutdown()

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[Assignment] shouldBe empty
    msgsOf[PiiUpdate] shouldBe empty
   }

  // Fix PiiId to stop the partitions for messages from changing all the time.
  val ourPiiId: ObjectId = new ObjectId( 10: Int, 7: Int, 9: Short, 56: Int )

  it should "an executor should shutdown correctly" in {

    val ex = makeExecutor(shutdownProcessStore)
    ex.executeWith(ourPiiId, ri, Seq(21))

    Thread.sleep(10.seconds.toMillis)
    ex.syncShutdown()

    // Dont consume, we need the outstanding messages to resume.
    val msgsOf = new MessageDrain( false )

    // We should just be waiting on the assignments.
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[PiiUpdate] should (have size 1)

    // Depending on whether the assignments ended up in different partitions.
    msgsOf[Assignment] should (have size 1 or have size 2)

    // We must be waiting on *ALL* active assignments.
    val calledIds: Seq[Int] = msgsOf[PiiUpdate].filter( _.pii.id == ourPiiId ).head.pii.called
    val assignedIds: Seq[Int] = msgsOf[Assignment].filter( _.pii.id == ourPiiId ).map( _.callRef.id )
    calledIds should contain theSameElementsAs assignedIds
  }

  it should "resume execution after a shutdown" in {

    val ex = makeExecutor( completeProcessStore )
    val f2: Future[(Y,Z)] = ex.connect( ourPiiId, ri, Seq(21) )._2
    pciw.continue()

    await(f2) should be (("PbISleptFor2s","PcISleptFor1s"))

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[Assignment] shouldBe empty
    msgsOf[PiiUpdate] shouldBe empty
  }

  it should "execute correctly with an outstanding PiiUpdate" in {

    // Construct and send an outstanding PiiUpdate message to test robustness.
    val oldPii: PiInstance[ObjectId] = PiInstance.forCall( ObjectId.get, ri, 2, 1 )
    KafkaConnectors.sendMessages( PiiUpdate( oldPii ) )( newSettings( completeProcessStore ) )

    val ex = makeExecutor(completeProcessStore)
    val f1 = ex.execute(ri, Seq(21))

    await(f1) should be (("PbISleptFor2s","PcISleptFor1s"))
    ex.syncShutdown()

    // We don't care what state we leave the outstanding message in, provided we clean our own state.
    val ourMsg: AnyMsg => Boolean = piiId(_) != oldPii.id

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest].filter( ourMsg ) shouldBe empty
    msgsOf[SequenceFailure].filter( ourMsg ) shouldBe empty
    msgsOf[ReduceRequest].filter( ourMsg ) shouldBe empty
    msgsOf[Assignment].filter( ourMsg ) shouldBe empty
    msgsOf[PiiUpdate].filter( ourMsg ) shouldBe empty
  }

  it should "execute correctly with an outstanding an *irreducible* PiiUpdate" in {

    // Construct and send a fully reduced PiiUpdate message to test robustness.
    val oldMsg: PiiUpdate
      = ( new Reducer ).piiReduce(
          PiInstance.forCall( ObjectId.get, ri, 2, 1 )
        ).collect({ case update: PiiUpdate => update }).head

    KafkaConnectors.sendMessages( oldMsg )( newSettings( completeProcessStore ) )

    val ex = makeExecutor(completeProcessStore)
    val f1 = ex.execute(ri, Seq(21))

    await(f1) should be (("PbISleptFor2s","PcISleptFor1s"))
    ex.syncShutdown()

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[Assignment] shouldBe empty
    msgsOf[PiiUpdate] should have size 1

    // TODO: PiInstances don't equal one another as they have different container types after serialization.
    // msgsOf[PiiUpdate].head shouldBe oldMsg
    msgsOf[PiiUpdate].head.pii.id shouldBe oldMsg.pii.id
  }
}
