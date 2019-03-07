package com.workflowfm.pew.stateless

import java.util.concurrent.TimeoutException

import akka.Done
import com.workflowfm.pew.stateless.StatelessMessages.{AnyMsg, _}
import com.workflowfm.pew.stateless.components.{AtomicExecutor, Reducer, ResultListener}
import com.workflowfm.pew.stateless.instances.kafka.CustomKafkaExecutor
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConnectors
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConnectors.{DrainControl, sendMessages}
import com.workflowfm.pew.stateless.instances.kafka.settings.{KafkaExecutorEnvironment, KafkaExecutorSettings}
import com.workflowfm.pew.{PiEventFinish, PromiseHandler, _}
import org.apache.kafka.common.utils.Utils
import org.bson.types.ObjectId
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent._
import scala.concurrent.duration._

//noinspection ZeroIndexToHead
@RunWith(classOf[JUnitRunner])
class KafkaExecutorTests
  extends FlatSpec with Matchers with KafkaTests {

  // Ensure there are no outstanding messages before starting testing.
  new MessageDrain( true )


  /////////////////////////
  // Class Loader Checks //
  /////////////////////////

  lazy val mainClassLoader: ClassLoader = Thread.currentThread().getContextClassLoader
  lazy val kafkaClassLoader: ClassLoader = Utils.getContextOrKafkaClassLoader
  lazy val threadClassLoader: ClassLoader = await(Future.unit.map(_ => Thread.currentThread().getContextClassLoader))

  "A KafkaExecutor" should "use the same ClassLoader for Kafka as the Main thread" in {
    mainClassLoader shouldBe kafkaClassLoader
  }

  // IGNORE: Instead unset the class loader when creating KafkaProducers.
  ignore should "use the same ClassLoader for the ExecutionContext threads as the Main thread" in {
    mainClassLoader shouldBe threadClassLoader
  }

  // TODO: Fix
  ignore should "should start a producer from inside a ExecutionContext" in {
    try {
      implicit val s: KafkaExecutorSettings = completeProcess.settings

      val future: Future[Done] = Future {
        Thread.sleep(100)
      } map { _ =>
        val pii = PiInstance(ObjectId.get, pbi, PiObject(1))
        // sendMessages(ReduceRequest(pii, Seq()))
        Done
      }

      await(future) shouldBe Done

    } finally {
      new MessageDrain(true)
    }
  }


  //////////////////////////////////////////
  // Helper functions for execution tests //
  //////////////////////////////////////////

  implicit def enhanceWithContainsDuplicates[T]( s: Seq[T] ) = new {
    def hasDuplicates: Boolean= s.distinct.size != s.size
  }

  /** Jev, a `tryBut/always` control like a `try/finally`, but returns
    * with the output of the `always` block instead of the `try` block.
    *
    * @param tryFn A code block which might throw errors.
    * @return The return value of `always` block, if no errors were thrown.
    */
  private def tryBut( tryFn: => Unit ) = {
    new {
      def always[T]( alwaysFn: => T ): T = {
        try {
          tryFn
          alwaysFn
        } catch {
          case throwable: Throwable =>
            alwaysFn
            throw throwable
        }
      }
    }
  }

  def ensureShutdownThen[T]( exec: CustomKafkaExecutor )( fnFinally: => T ): T = {
    try {
      exec.syncShutdown(20.seconds)

    } catch {
      case ex: TimeoutException =>
        Await.result( exec.forceShutdown, Duration.Inf )
        fnFinally
        throw ex
    }

    tryBut {
      for (control <- exec.allControls) {
        assert(control.isShutdown.isCompleted, s"All controls ($control) should be shutdown.")
      }
    } always {
      fnFinally
    }
  }

  def withClueMessageCounts[T](msgMap: MessageMap)(fun: => T): T = {
    withClue(clue =
      s"#SequenceRequests: ${msgMap[SequenceRequest].length}\n" +
      s"#SequenceFailures: ${msgMap[SequenceFailure].length}\n" +
      s"#ReduceRequests:   ${msgMap[ReduceRequest].length}\n" +
      s"#Assignments:      ${msgMap[Assignment].length}\n" +
      s"#PiiUpdates:       ${msgMap[PiiUpdate].length}\n"
    )(fun)
  }

  def checkOutstandingMsgs(msgMap: MessageMap): Unit = {
    withClueMessageCounts(msgMap) {
      withClue(s"It shouldn't have any outstanding messsages.\n") {
        msgMap[SequenceRequest] ++ msgMap[SequenceFailure] ++ msgMap[ReduceRequest] ++
          msgMap[Assignment] ++ msgMap[PiiUpdate] shouldBe empty
      }
    }
  }

  def checkForUnmatchedLogs(msgsOf: => MessageMap): Unit = {
    lazy val logsOf = toLogMap( msgsOf )

    val nPiiEvent = logsOf[PiEvent[_]].length
    val nPiiStart = logsOf[PiEventStart[_]].length
    val nPiiFinish = logsOf[PiEventFinish[_]].length
    val nPiiResult = logsOf[PiEventResult[_]].length
    val nPiiNoRes = logsOf[PiFailureNoResult[_]].length
    val nPiiUnkProc = logsOf[PiFailureUnknownProcess[_]].length
    val nPiiIsComp = logsOf[PiFailureAtomicProcessIsComposite[_]].length
    val nPiiNoInst = logsOf[PiFailureNoSuchInstance[_]].length
    val nPiiExceps = logsOf[PiFailureExceptions[_]].length
    val nPiiCall = logsOf[PiEventCall[_]].length
    val nPiiCallEnd = logsOf[PiEventCall[_]].length
    val nPiiReturn = logsOf[PiEventReturn[_]].length
    val nPiiAPExceps = logsOf[PiFailureAtomicProcessException[_]].length

    // Print PiiEvent breakdown when any of the match checks fails.
    withClue( clue =
      s"#Event:     $nPiiEvent\n" +
      s"#Start*:    $nPiiStart\n" +
      s"#Finish:    $nPiiFinish\n" +
      s"#Result*:   $nPiiResult\n" +
      s"#NoRes*:    $nPiiNoRes\n" +
      s"#UnkProc*:  $nPiiUnkProc\n" +
      s"#IsComp*:   $nPiiIsComp\n" +
      s"#NoInst*:   $nPiiNoInst\n" +
      s"#Exceps*:   $nPiiExceps\n" +
      s"#Call*:     $nPiiCall\n" +
      s"#CallEnd:   $nPiiCallEnd\n" +
      s"#Return*:   $nPiiReturn\n" +
      s"#ApExceps*: $nPiiAPExceps\n"
    ) {

      withClue( s"Test arithmatic includes all events:" ) {
        nPiiEvent shouldBe (
          nPiiStart + nPiiResult + nPiiNoRes + nPiiUnkProc + nPiiIsComp +
          nPiiNoInst + nPiiExceps + nPiiCall + nPiiReturn + nPiiAPExceps
        )
      }

      val allStartedPiis = logsOf[PiEventStart[_]].map(_.id)
      val allFinishedPiis = logsOf[PiEventFinish[_]].map(_.id)

      withClue( s"PiInstances should only start once:") {
        assert( !allStartedPiis.hasDuplicates, "no duplicate PiEventStarts" )
      }

      withClue( s"PiInstances should only end once:") {
        assert( !allFinishedPiis.hasDuplicates, "no duplicate PiEventFinishes" )
      }

      withClue(s"PiEventStarts need a corresponding PiEventFinish:") {
        allStartedPiis should contain theSameElementsAs allFinishedPiis
      }

      withClue("There should have at least 1 subcall for each start:") {
        nPiiStart should be <= nPiiCall

      }

      val allStartedCalls = logsOf[PiEventCall[_]].map(e => (e.id, e.ref))
      val allFinishedCalls = logsOf[PiEventCallEnd[_]].map(e => (e.id, e.ref))

      withClue("Every PiEventCall should have a unique call reference:") {
        assert( !allStartedCalls.hasDuplicates, "no duplicate PiEventCalls" )
      }

      withClue("Every PiEventCallEnd should have a unique call reference:") {
        assert( !allFinishedCalls.hasDuplicates, "no duplicate PiEventCallEnds" )
      }

      withClue("PiEventCalls need a corresponding PiEventCallEnd:") {
        allStartedCalls should contain theSameElementsAs allFinishedCalls
      }

      withClue("Every PiEventCall should belong to an executed PiInstance:") {
        val allCalls = logsOf[PiEventCall[_]].map(_.id).toSet
        val allStarts = logsOf[PiEventCall[_]].map(_.id).toSet
        allStarts should contain allElementsOf allCalls
      }
    }
  }


  /////////////////////////
  // All Execution Tests //
  /////////////////////////

  class DiyInterface {
    implicit val s: KafkaExecutorSettings = completeProcess.settings
    implicit val env: KafkaExecutorEnvironment = s.createEnvironment()
    private val listener = new ResultListener

    private val controls: Seq[DrainControl] = Seq(
      KafkaConnectors.indyReducer(new Reducer),
      KafkaConnectors.indySequencer,
      KafkaConnectors.indyAtomicExecutor(new AtomicExecutor()),
      KafkaConnectors.uniqueResultListener(listener),
    )

    def call( p: PiProcess, args: PiObject* ): Future[Any] = {
      val pii = PiInstance(ObjectId.get, p, args: _*)

      val handler = new PromiseHandler("test", pii.id)
      listener.subscribe(handler)

      sendMessages(ReduceRequest(pii, Seq()), PiiLog(PiEventStart(pii)))
      handler.promise.future
    }

    def shutdown(): Unit = {
      await( KafkaConnectors.shutdownAll(controls) )
    }
  }

  it should "call an atomic PbI (DIY interface)" in {

    val msgsOf: MessageMap = {
      val diyEx = new DiyInterface

      tryBut {
        val f1 = diyEx.call(pbi, PiObject(1))
        await(f1) should be("PbISleptFor1s")

      } always {
        diyEx.shutdown()
        new MessageDrain(true)
      }
    }

    checkOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  it should "call an Rexample (DIY interface)" in {
    val msgsOf: MessageMap = {
      val diyEx = new DiyInterface

      tryBut {
        val f1 = diyEx.call(ri, PiObject(21))
        await(f1) should be(("PbISleptFor2s", "PcISleptFor1s"))

      } always {
        diyEx.shutdown()
        new MessageDrain(true)
      }
    }

    checkOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  def baremetalCall( ex: CustomKafkaExecutor, p: PiProcess, args: PiObject*  ): Future[Any] = {
    val piiId = await( ex.init( p, args.toSeq ) )
    val handler = new PromiseHandler("test", piiId)
    ex.subscribe(handler)

    ex.start(piiId)
    handler.promise.future
  }

  it should "call an atomic PbI (baremetal interface)" in {
    val msgsOf: MessageMap  = {
      val ex = makeExecutor(completeProcess.settings)

      tryBut {
        val f1 = baremetalCall( ex, pbi, PiObject(1) )
        await(f1) should be("PbISleptFor1s")

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    checkOutstandingMsgs( msgsOf )
    checkForUnmatchedLogs( msgsOf )
  }

  it should "call an Rexample (baremetal interface)" in {
    val msgsOf: MessageMap  = {
      val ex = makeExecutor(completeProcess.settings)

      tryBut {
        val f1 = baremetalCall( ex, ri, PiObject(21) )
        await(f1) should be(("PbISleptFor2s", "PcISleptFor1s"))

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    checkOutstandingMsgs( msgsOf )
    checkForUnmatchedLogs( msgsOf )
  }

  it should "call an atomic PbI" in {
    val msgsOf: MessageMap  = {
      val ex = makeExecutor(completeProcess.settings)

      tryBut {
        await( ex.execute(pbi, Seq(1)) ) should be("PbISleptFor1s")

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    checkOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  it should "call 2 concurrent atomic PbI" in {
    val msgsOf: MessageMap  = {
      val ex = makeExecutor(completeProcess.settings)

      tryBut {
        val f1 = ex.execute(pbi, Seq(2))
        val f2 = ex.execute(pbi, Seq(1))

        await(f1) should be("PbISleptFor2s")
        await(f2) should be("PbISleptFor1s")

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    checkOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  it should "call an Rexample" in {
    val msgsOf: MessageMap  = {
      val ex = makeExecutor(completeProcess.settings)

      tryBut {
        await(ex.execute(ri, Seq(21))) should be(("PbISleptFor2s", "PcISleptFor1s"))

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    checkOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  it should "call an Rexample with same timings" in {
    val msgsOf: MessageMap  = {
      val ex = makeExecutor(completeProcess.settings)

      tryBut {
        await(ex.execute(ri, Seq(11))) should be(("PbISleptFor1s", "PcISleptFor1s"))

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    checkOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  it should "call 2 concurrent Rexamples" in {
    val msgsOf: MessageMap  = {
      val ex = makeExecutor(completeProcess.settings)

      tryBut {
        val f1 = ex.execute(ri, Seq(31))
        val f2 = ex.execute(ri, Seq(12))

        await(f1) should be(("PbISleptFor3s", "PcISleptFor1s"))
        await(f2) should be(("PbISleptFor1s", "PcISleptFor2s"))

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    checkOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  it should "call 2 concurrent Rexamples with same timings" in {
    val msgsOf: MessageMap  = {
      val ex = makeExecutor(completeProcess.settings)

      tryBut {
        val f1 = ex.execute(ri, Seq(11))
        val f2 = ex.execute(ri, Seq(11))

        await(f1) should be(("PbISleptFor1s", "PcISleptFor1s"))
        await(f2) should be(("PbISleptFor1s", "PcISleptFor1s"))

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    checkOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  it should "call 2 sequential Rexamples" in {
    val msgsOf: MessageMap = {
      val ex = makeExecutor(completeProcess.settings)

      tryBut {
        await(ex.execute(ri, Seq(21))) should be(("PbISleptFor2s", "PcISleptFor1s"))
        await(ex.execute(ri, Seq(21))) should be(("PbISleptFor2s", "PcISleptFor1s"))

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    checkOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  it should "call 3 concurrent Rexamples" in {
    val msgsOf: MessageMap  = {
      val ex = makeExecutor(completeProcess.settings)

      tryBut {
        val f1 = ex.execute(ri, Seq(11))
        val f2 = ex.execute(ri, Seq(11))
        val f3 = ex.execute(ri, Seq(11))

        await(f1) should be(("PbISleptFor1s", "PcISleptFor1s"))
        await(f2) should be(("PbISleptFor1s", "PcISleptFor1s"))
        await(f3) should be(("PbISleptFor1s", "PcISleptFor1s"))

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    checkOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  it should "call 2 Rexamples each with different processes" in {
    val msgsOf: MessageMap  = {
      val ex = makeExecutor(completeProcess.settings)

      tryBut {
        val f1 = ex.execute(ri, Seq(11))
        val f2 = ex.execute(ri2, Seq(11))

        await(f1) should be(("PbISleptFor1s", "PcISleptFor1s"))
        await(f2) should be(("PbISleptFor1s", "PcXSleptFor1s"))

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    checkOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  it should "call 2 concurrent Rexamples on different executors with same timings" in {

    val msgsOf: MessageMap = {
      val ex1 = makeExecutor(completeProcess.settings)
      val ex2 = makeExecutor(completeProcess.settings)

      tryBut {
        val f1 = ex1.execute(ri, Seq(11))
        val f2 = ex2.execute(ri, Seq(11))

        await(f1) should be(("PbISleptFor1s", "PcISleptFor1s"))
        await(f2) should be(("PbISleptFor1s", "PcISleptFor1s"))

      } always {
        ensureShutdownThen(ex1) {
          ensureShutdownThen(ex2) {
            new MessageDrain(true)
          }
        }
      }
    }

    checkOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  it should "call a failed atomic process" in {
    val msgsOf: MessageMap = {
      val ex = makeExecutor(failureProcess.settings)

      tryBut {
        val f1 = ex.execute(failp, Seq(1))
        a[RemoteException[ObjectId]] should be thrownBy await(f1)

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    checkOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  it should "call a failed composite process" in {
    val msgsOf: MessageMap = {
      val ex = makeExecutor(failureProcess.settings)

      tryBut {
        val f1 = ex.execute(rif, Seq(21))
        a[RemoteException[ObjectId]] should be thrownBy await(f1)

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    checkOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  it should "call an Rexample after calling a failed process" in {
    val msgsOf: MessageMap = {
      tryBut {

        val ex1 = makeExecutor(failureProcess.settings)
        try {
          val f1 = ex1.execute(rif, Seq(21))
          a[RemoteException[ObjectId]] should be thrownBy await(f1)
        } finally ensureShutdownThen(ex1) {}

        val ex2 = makeExecutor(completeProcess.settings)
        try {
          val f2 = ex2.execute(ri, Seq(21))
          await(f2) should be(("PbISleptFor2s", "PcISleptFor1s"))
        } finally ensureShutdownThen(ex2) {}

      } always {
        new MessageDrain(true)
      }
    }

    checkOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  it should "throw an exception when used after being shutdown" in {
    val msgsOf: MessageMap = {
      tryBut {
        val ex = makeExecutor(completeProcess.settings)
        ensureShutdownThen( ex ) {}
        a[ShutdownExecutorException] should be thrownBy ex.execute(rif, Seq(21))

      } always {
        new MessageDrain(true)
      }
    }

    withClue( "No execution should've been performed." ) {
      msgsOf[AnyMsg] shouldBe empty
    }
  }

  it should "not complete execution of an AtomicProcess after a forced shutdown" in {
    val onShutdown: MessageMap = {
      tryBut {
        val ex = makeExecutor(shutdownProcess.settings)
        ex.execute(pci, Seq(1))

        Thread.sleep(5.seconds.toMillis)
        await(ex.forceShutdown)

        pciw.fail()

      } always {
        new MessageDrain(true)
      }
    }

    withClueMessageCounts(onShutdown) {
      withClue("There should be no SequenceRequests, SequenceFailures, or ReduceRequests.\n") {
        onShutdown[SequenceRequest] shouldBe empty
        onShutdown[SequenceFailure] shouldBe empty
        onShutdown[ReduceRequest] shouldBe empty
      }
      withClue( "There should be 1 Assignment and 1 PiiUpdate.\n") {
        onShutdown[Assignment] should have size 1
        onShutdown[PiiUpdate] should have size 1
      }
    }
  }

  def testShutdown( process: PiProcess, args: Seq[Any], expectedResult: Any ): Unit = {

    val ourPiiId: ObjectId = {
      try {
        val ex = makeExecutor(shutdownProcess.settings)
        val futId: Future[ObjectId] = ex.call(process, args)

        Thread.sleep(10.seconds.toMillis)
        await(ex.forceShutdown) // `forceShutdown` so it doesn't drain and produce.

        pciw.fail() // To free the locked AtomicProcessExecutor thread.

        // The future is created with Future.success.
        futId.value.get.get

      } catch {
        case ex: Throwable =>
          new MessageDrain(true)
          throw ex
      }
    }

    // Dont consume, we need the outstanding messages to resume.
    val atShutdown: MessageMap = new MessageDrain(false)

    val onCompletion: MessageMap = {

      val handler = new PromiseHandler[ObjectId]("testhandler", ourPiiId)
      val ex2 = makeExecutor(completeProcess.settings)

      tryBut {
        ex2.subscribe(handler)
        pciw.continue()
        await(handler.future) shouldBe expectedResult

      } always {
        ensureShutdownThen(ex2) {
          new MessageDrain(true)
        }
      }
    }

    withClue("Unexpected messages after the shutdown:\n") {
      withClueMessageCounts( atShutdown ) {

        withClue( s"All messages should belong our PiInstance ($ourPiiId).\n" ) {
          all( atShutdown[AnyMsg] map (_.piiId) ) shouldBe ourPiiId
        }

        withClue( "There should be no SequenceRequests, SequenceFailures or ReduceRequests:\n") {
          atShutdown[SequenceRequest] ++ atShutdown[SequenceFailure] ++
            atShutdown[ReduceRequest] shouldBe empty
        }

        withClue("There should be exactly 1 outstanding PiiUpdate:\n") {
          atShutdown[PiiUpdate] should have size 1
        }

        withClue("There should be 1 or 2 outstanding Assignments") {
          // Depending on whether the assignments ended up in different partitions.
          atShutdown[Assignment] should (have size 1 or have size 2)
        }

        withClue("It should be waiting on *ALL* active assignments.") {
          val calledIds: Set[Int] = atShutdown[PiiUpdate].head.pii.called.toSet
          val assignedIds: Set[Int] = atShutdown[Assignment].map(_.callRef.id).toSet
          calledIds should contain theSameElementsAs assignedIds
        }
      }
    }

    withClue(s"After completing PiInstance ($ourPiiId) execution:\n") {
      checkOutstandingMsgs(onCompletion)
      checkForUnmatchedLogs(onCompletion)
    }
  }

  // Jev, use `PcI` as it's configured to `PPcIWait` under shutdown settings.
  it should "call an atomic PcI interupted by a shutdown" in {
    testShutdown( pci, Seq(1), "PcISleptFor1s" )
  }

  it should "call an Rexample interupted by a shutdown." in {
    testShutdown( ri, Seq(21), ("PbISleptFor2s", "PcISleptFor1s") )
  }

  it should "call an Rexample (with an outstanding PiiUpdate)" in {

    // Construct and send an outstanding PiiUpdate message to test robustness.
    val oldPii: PiInstance[ObjectId] = PiInstance.forCall(ObjectId.get, ri, 2, 1)

    val msgsOf: MessageMap = {
      val ex = makeExecutor(completeProcess.settings)

      tryBut {
        KafkaConnectors.sendMessages(PiiUpdate(oldPii))(ex.environment)

        val f1 = ex.execute(ri, Seq(21))
        await(f1) should be(("PbISleptFor2s", "PcISleptFor1s"))

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    // We don't care what state we leave the outstanding message in,
    // it is sufficient that we clean our own state.
    checkOutstandingMsgs(msgsOf filter (_.piiId != oldPii.id))
  }

  it should "call an Rexample (with an outstanding *irreducible* PiiUpdate)" in {

    val oldMsg: PiiUpdate
      = PiiUpdate(
        PiInstance(ObjectId.get, ri, PiObject(13))
          .reduce
          .handleThreads((_, _) => true)._2
      )

    // Construct and send a fully reduced PiiUpdate message to test robustness.
    //    val oldMsg: PiiUpdate
    //      = ( new Reducer ).piiReduce(
    //          PiInstance.forCall( ObjectId.get, ri, 2, 1 )
    //        ).collect({ case update: PiiUpdate => update }).head

    val msgsOf: MessageMap = {
      val ex = makeExecutor(completeProcess.settings)

      tryBut {
        KafkaConnectors.sendMessages(oldMsg)(ex.environment)

        val f1 = ex.execute(ri, Seq(21))
        await(f1) should be(("PbISleptFor2s", "PcISleptFor1s"))

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    checkOutstandingMsgs(msgsOf filter (!_.isInstanceOf[PiiUpdate]))

    withClue( "The irreducible PiUpdate shouldn't be processed:" ) {
      msgsOf[PiiUpdate] should have size 1

      // TODO: PiInstances don't equal one another as they have different container types after serialization.
      // msgsOf[PiiUpdate].head shouldBe oldMsg
      msgsOf[PiiUpdate].head.pii.id shouldBe oldMsg.pii.id
    }
  }

  it should "call Rexamples under heavy load." in {

    val msgsOf: MessageMap = {
      val ex = makeExecutor(completeProcess.settings)

      tryBut {
        for (i <- 0 to 240) {

          // Jev, intersperse some timeconsuming tasks.
          val b: Seq[Int]
            = Seq(41, 43, 47, 53, 59, 61)
              .map(j => if ((i % j) == 0) 1 else 0)

          val f0 = ex.execute(ri, Seq(b(0) * 10 + b(1)))
          val f1 = ex.execute(ri, Seq(b(2) * 10 + b(3)))
          val f2 = ex.execute(ri, Seq(b(4) * 10 + b(5)))

          // Allow more time initially as 3 are running at once.
          await(f0, 45.seconds) shouldBe (s"PbISleptFor${b(0)}s", s"PcISleptFor${b(1)}s")
          await(f1, 30.seconds) shouldBe (s"PbISleptFor${b(2)}s", s"PcISleptFor${b(3)}s")
          await(f2, 15.seconds) shouldBe (s"PbISleptFor${b(4)}s", s"PcISleptFor${b(5)}s")
        }

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    checkForUnmatchedLogs(msgsOf)
    checkOutstandingMsgs(msgsOf)
  }
}
