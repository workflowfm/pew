package com.workflowfm.pew.stateless

import akka.Done
import com.workflowfm.pew.stateless.StatelessMessages.{AnyMsg, _}
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConnectors
import com.workflowfm.pew.{PromiseHandler, _}
import com.workflowfm.pew.stateless.components.{AtomicExecutor, Reducer, ResultListener}
import com.workflowfm.pew.stateless.instances.kafka.MinimalKafkaExecutor
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConnectors.{indyReducer, indySequencer, sendMessages}
import org.apache.kafka.common.utils.Utils
import org.bson.types.ObjectId
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.concurrent._
import scala.concurrent.duration._

//noinspection ZeroIndexToHead
@RunWith(classOf[JUnitRunner])
class KafkaExecutorTests extends PewTestSuite with KafkaTests {

  // Ensure there are no outstanding messages before starting testing.
  new MessageDrain( true )

  lazy val mainClassLoader: ClassLoader = Thread.currentThread().getContextClassLoader
  lazy val kafkaClassLoader: ClassLoader = Utils.getContextOrKafkaClassLoader
  lazy val threadClassLoader: ClassLoader = await( Future.unit.map(_ => Thread.currentThread().getContextClassLoader ) )

  it should "use the same ClassLoader for Kafka as the Main thread" in {
    mainClassLoader shouldBe kafkaClassLoader
  }

  // Instead unset the class loader when creating KafkaProducers.
  ignore should "use the same ClassLoader for the ExecutionContext threads as the Main thread" in {
    mainClassLoader shouldBe threadClassLoader
  }

  it should "start a producer from inside a ExecutionContext" in {
    implicit val settings = completeProcess.settings

    val future: Future[Done] = Future { Thread.sleep(100) } map { _ =>
      val pii = PiInstance( ObjectId.get, pbi, PiObject(1) )
      sendMessages( ReduceRequest( pii, Seq() ) )
      Done
    }

    await( future ) shouldBe Done
  }

  it should "execute an atomic PbI using a DIY KafkaExecutor" in {
    implicit val settings = completeProcess.settings
    val listener = new ResultListener

    val c1 = KafkaConnectors.indyReducer( new Reducer )
    val c2 = KafkaConnectors.indySequencer
    val c3 = KafkaConnectors.indyAtomicExecutor( AtomicExecutor() )
    val c4 = KafkaConnectors.uniqueResultListener( listener )

    val pii = PiInstance( ObjectId.get, pbi, PiObject(1) )
    sendMessages( ReduceRequest( pii, Seq() ) )

    val handler = new PromiseHandler( "test", pii.id )
    listener.subscribe( handler )

    await( handler.promise.future ) should be ("PbISleptFor1s")
    await( KafkaConnectors.shutdown( c1, c2, c3, c4 ) )

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[Assignment] shouldBe empty
    msgsOf[PiiUpdate] shouldBe empty
  }

  it should "execute an atomic PbI using the baremetal Executor interface" in {
    val ex = makeExecutor( completeProcess.settings )

    val piiId = await( ex.init( pbi, Seq( PiObject(1) ) ) )
    val handler = new PromiseHandler( "test", piiId )
    ex.subscribe( handler )

    val f1 = handler.promise.future
    ex.start( piiId )

    await( f1 ) should be ("PbISleptFor1s")
    ex.syncShutdown()

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[Assignment] shouldBe empty
    msgsOf[PiiUpdate] shouldBe empty  }

  it should "execute atomic PbI once" in {

    val ex = makeExecutor( completeProcess.settings )
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

    val ex = makeExecutor( completeProcess.settings )
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

    val ex = makeExecutor( completeProcess.settings )
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

    val ex = makeExecutor( completeProcess.settings )
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

    val ex = makeExecutor( completeProcess.settings )
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

    val ex = makeExecutor( completeProcess.settings )
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

    val ex = makeExecutor( completeProcess.settings )
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

    val ex = makeExecutor( completeProcess.settings )
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

    val ex = makeExecutor( failureProcess.settings )
    val f1 = ex.execute( failp, Seq(1) )

    a [RemoteProcessException[ObjectId]] should be thrownBy await(f1)
    ex.syncShutdown()

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[Assignment] shouldBe empty
    msgsOf[PiiUpdate] shouldBe empty
  }

  it should "handle a failing composite process" in {

    val ex = makeExecutor( failureProcess.settings )
    val f1 = ex.execute( rif, Seq(21) )

    a [RemoteProcessException[ObjectId]] should be thrownBy await(f1)
    ex.syncShutdown()

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[Assignment] shouldBe empty
    msgsOf[PiiUpdate] shouldBe empty
  }

  it should "2 separate executors should execute with same timings concurrently" in {

    val ex1 = makeExecutor( completeProcess.settings )
    val ex2 = makeExecutor( completeProcess.settings )

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

  lazy val fixShutdown1 = new {

    val ourPiiId: ObjectId = {
      val ex = makeExecutor( shutdownProcess.settings )
      val futId: Future[ObjectId] = ex.call(ri, Seq(21))

      Thread.sleep(10.seconds.toMillis)
      ex.syncShutdown()

      // The future is created with Future.success.
      futId.value.get.get
    }

    val handler = new PromiseHandler[ObjectId]( "testhandler", ourPiiId )

    // Dont consume, we need the outstanding messages to resume.
    val fstMsgs: MessageMap = new MessageDrain( false )

    {
      val ex = makeExecutor( completeProcess.settings )
      ex.subscribe(handler)
      pciw.continue()
    }

    val result: Any = await( handler.future )
    val sndMsgs: MessageMap = new MessageDrain( true )
  }

  it should "an executor should shutdown correctly" in {

    val f = fixShutdown1

    // We should just be waiting on the assignments.
    f.fstMsgs[ReduceRequest] shouldBe empty
    f.fstMsgs[SequenceRequest] shouldBe empty
    f.fstMsgs[SequenceFailure] shouldBe empty
    f.fstMsgs[PiiUpdate] should (have size 1)

    // Depending on whether the assignments ended up in different partitions.
    f.fstMsgs[Assignment] should (have size 1 or have size 2)

    // We must be waiting on *ALL* active assignments.
    val calledIds: Seq[Int] = f.fstMsgs[PiiUpdate].filter( _.pii.id == f.ourPiiId ).head.pii.called
    val assignedIds: Seq[Int] = f.fstMsgs[Assignment].filter( _.pii.id == f.ourPiiId ).map( _.callRef.id )
    calledIds should contain theSameElementsAs assignedIds
  }

  it should "resume execution after a shutdown" in {

    val f = fixShutdown1

    f.result should be (("PbISleptFor2s","PcISleptFor1s"))

    f.sndMsgs[SequenceRequest] shouldBe empty
    f.sndMsgs[SequenceFailure] shouldBe empty
    f.sndMsgs[ReduceRequest] shouldBe empty
    f.sndMsgs[Assignment] shouldBe empty
    f.sndMsgs[PiiUpdate] shouldBe empty
  }

  it should "execute correctly with an outstanding PiiUpdate" in {

    // Construct and send an outstanding PiiUpdate message to test robustness.
    val oldPii: PiInstance[ObjectId] = PiInstance.forCall( ObjectId.get, ri, 2, 1 )
    KafkaConnectors.sendMessages( PiiUpdate( oldPii ) )( completeProcess.settings )

    val ex = makeExecutor( completeProcess.settings )
    val f1 = ex.execute(ri, Seq(21))

    await(f1) should be (("PbISleptFor2s","PcISleptFor1s"))
    ex.syncShutdown()

    // We don't care what state we leave the outstanding message in, provided we clean our own state.
    val ourMsg: AnyMsg => Boolean = _.piiId != oldPii.id

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest].filter( ourMsg ) shouldBe empty
    msgsOf[SequenceFailure].filter( ourMsg ) shouldBe empty
    msgsOf[ReduceRequest].filter( ourMsg ) shouldBe empty
    msgsOf[Assignment].filter( ourMsg ) shouldBe empty
    msgsOf[PiiUpdate].filter( ourMsg ) shouldBe empty
  }

  it should "execute correctly with an outstanding an *irreducible* PiiUpdate" in {

    val oldMsg: PiiUpdate
      = PiiUpdate(
        PiInstance( ObjectId.get, ri, PiObject(13) )
        .reduce
        .handleThreads((_, _) => true)._2
      )

    // Construct and send a fully reduced PiiUpdate message to test robustness.
//    val oldMsg: PiiUpdate
//      = ( new Reducer ).piiReduce(
//          PiInstance.forCall( ObjectId.get, ri, 2, 1 )
//        ).collect({ case update: PiiUpdate => update }).head

    KafkaConnectors.sendMessages( oldMsg )( completeProcess.settings )

    val ex = makeExecutor( completeProcess.settings )
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

  it should "execute correctly under load" in {

    val ex = makeExecutor( completeProcess.settings )
    var errors: List[Exception] = List()

    try {
      for (i <- 0 to 240) {

        // Jev, intersperse some timeconsuming tasks.
        val b: Seq[Int]
          = Seq( 41, 43, 47, 53, 59, 61 )
            .map( j => if ((i % j) == 0) 1 else 0 )

        val f0 = ex.execute(ri, Seq( b(0) * 10 + b(1) ) )
        val f1 = ex.execute(ri, Seq( b(2) * 10 + b(3) ) )
        val f2 = ex.execute(ri, Seq( b(4) * 10 + b(5) ) )

        await(f0) shouldBe (s"PbISleptFor${b(0)}s", s"PcISleptFor${b(1)}s")
        await(f1) shouldBe (s"PbISleptFor${b(2)}s", s"PcISleptFor${b(3)}s")
        await(f2) shouldBe (s"PbISleptFor${b(4)}s", s"PcISleptFor${b(5)}s")
      }

      ex.syncShutdown()

    } catch {
      case e: Exception =>
        e.printStackTrace()
        errors = e :: errors
    }

    val msgsOf = new MessageDrain( true )
    msgsOf[SequenceRequest] shouldBe empty
    msgsOf[SequenceFailure] shouldBe empty
    msgsOf[ReduceRequest] shouldBe empty
    msgsOf[Assignment] shouldBe empty
    msgsOf[PiiUpdate] shouldBe empty

    errors shouldBe empty
  }
}
