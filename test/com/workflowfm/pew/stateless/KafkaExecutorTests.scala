package com.workflowfm.pew.stateless

import java.util.concurrent.TimeoutException

import akka.Done
import com.workflowfm.pew.stateless.StatelessMessages.{AnyMsg, _}
import com.workflowfm.pew.stateless.components.{AtomicExecutor, Reducer, ResultListener}
import com.workflowfm.pew.stateless.instances.kafka.CustomKafkaExecutor
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConnectors
import com.workflowfm.pew.stateless.instances.kafka.components.KafkaConnectors.{DrainControl, sendMessages}
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.BsonKafkaExecutorSettings
import com.workflowfm.pew.{PromiseHandler, _}
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

  it should "should start a producer from inside a ExecutionContext" in {
    implicit val settings = completeProcess.settings

    val future: Future[Done] = Future {
      Thread.sleep(100)
    } map { _ =>
      val pii = PiInstance(ObjectId.get, pbi, PiObject(1))
      sendMessages(ReduceRequest(pii, Seq()))
      Done
    }

    await(future) shouldBe Done
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

  def checkForOutstandingMsgs(msgsOf: => MessageMap): Unit = {
    assert(msgsOf[SequenceRequest].isEmpty, "it shouldn't have outstanding SequenceRequests." )
    assert(msgsOf[SequenceFailure].isEmpty, "it shouldn't have outstanding SequenceFailures." )
    assert(msgsOf[ReduceRequest].isEmpty, "it shouldn't have outstanding ReduceRequests." )
    assert(msgsOf[Assignment].isEmpty, "it shouldn't have outstanding Assignments." )
    assert(msgsOf[PiiUpdate].isEmpty, "it shouldn't have outstanding PiiUpdates." )
  }

  def checkForUnmatchedLogs(msgsOf: => MessageMap): Unit = {
    lazy val logsOf = toLogMap( msgsOf )

    lazy val nPiiStarts = logsOf[PiEventStart[_]].length
    lazy val nPiiCalls = logsOf[PiEventCall[_]].length

    withClue("PiEventStarts need a corresponding PiEventFinish:") {
      nPiiStarts shouldBe logsOf[PiEventFinish[_]].length
    }

    withClue("PiEventCalls need a corresponding PiEventCallEnd:") {
      nPiiCalls shouldBe logsOf[PiEventCallEnd[_]].length
    }

    withClue("There should have at least 1 subcall for each start:") {
      nPiiStarts should be <= nPiiCalls
    }

    withClue("Every PiEventCall should have a unique call reference:") {
      val nUniqueCalls = logsOf[PiEventCall[_]].map(e => (e.id, e.ref)).toSet.size
      nUniqueCalls shouldBe nPiiCalls
    }

    withClue("Every PiEventCall should belong to an executed PiInstance:") {
      val allCalls = logsOf[PiEventCall[_]].map(_.id).toSet
      val allStarts = logsOf[PiEventCall[_]].map(_.id).toSet
      allStarts should contain allElementsOf allCalls
    }
  }

  it should "call an atomic PbI (DIY interface)" in {
    implicit val settings: BsonKafkaExecutorSettings = completeProcess.settings
    val listener = new ResultListener

    val msgsOf: MessageMap = {
      val controls: Seq[DrainControl]
        = Seq(
          KafkaConnectors.indyReducer(new Reducer),
          KafkaConnectors.indySequencer,
          KafkaConnectors.indyAtomicExecutor(new AtomicExecutor()),
          KafkaConnectors.uniqueResultListener(listener),
        )

      tryBut {
        val pii = PiInstance(ObjectId.get, pbi, PiObject(1))
        sendMessages(ReduceRequest(pii, Seq()), PiiLog(PiEventStart(pii)))

        val handler = new PromiseHandler("test", pii.id)
        listener.subscribe(handler)

        await(handler.promise.future) should be("PbISleptFor1s")

      } always {
        await( KafkaConnectors.shutdownAll(controls) )
        new MessageDrain(true)
      }
    }

    checkForOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  it should "call an atomic PbI (baremetal interface)" in {
    val msgsOf: MessageMap  = {
      val ex = makeExecutor(completeProcess.settings)

      tryBut {
        val piiId = await(ex.init(pbi, Seq(PiObject(1))))
        val handler = new PromiseHandler("test", piiId)
        ex.subscribe(handler)

        ex.start(piiId)
        await(handler.promise.future) should be("PbISleptFor1s")

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    checkForOutstandingMsgs( msgsOf )
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

    checkForOutstandingMsgs(msgsOf)
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

    checkForOutstandingMsgs(msgsOf)
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

    checkForOutstandingMsgs(msgsOf)
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

    checkForOutstandingMsgs(msgsOf)
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

    checkForOutstandingMsgs(msgsOf)
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

    checkForOutstandingMsgs(msgsOf)
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

    checkForOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  it should "call 2 Rexamples each with a different component" in {
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

    checkForOutstandingMsgs(msgsOf)
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

    checkForOutstandingMsgs(msgsOf)
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

    checkForOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  it should "call an Rexample after calling a failed process" in {
    pending
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

    checkForOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }

  it should "call an Rexample interupted with shutdown." in {

    val ourPiiId: ObjectId = {

      val ex = makeExecutor(shutdownProcess.settings)
      val futId: Future[ObjectId] = ex.call(ri, Seq(21))

      Thread.sleep(10.seconds.toMillis)
      await( ex.forceShutdown ) // This won't drain so use forced shutdown.

      // The future is created with Future.success.
      futId.value.get.get
    }

    // Dont consume, we need the outstanding messages to resume.
    val fstMsgs: MessageMap = new MessageDrain(false)

    val sndMsgs: MessageMap = {

      val handler = new PromiseHandler[ObjectId]("testhandler", ourPiiId)
      val ex2 = makeExecutor(completeProcess.settings)

      tryBut {
        ex2.subscribe(handler)
        pciw.continue()

        await(handler.future) should be(("PbISleptFor2s", "PcISleptFor1s"))

      } always {
        ensureShutdownThen(ex2) {
          new MessageDrain(true)
        }
      }
    }

    // We should only be waiting on PiiUpdates or Assignments.
    def shouldBeEmpty(m: AnyMsg): Boolean = !(m.isInstanceOf[PiiUpdate] || m.isInstanceOf[Assignment])
    checkForOutstandingMsgs(fstMsgs.filter(shouldBeEmpty))

    withClue("produces exactly one outstanding PiiUpdate") {
      fstMsgs[PiiUpdate] should (have size 1)
    }

    withClue("produces either 1 or 2 outstanding Assignments") {
      // Depending on whether the assignments ended up in different partitions.
      fstMsgs[Assignment] should (have size 1 or have size 2)
    }

    withClue("should be waiting on *ALL* active assignments.") {
      val calledIds: Seq[Int] = fstMsgs[PiiUpdate].filter(_.pii.id == ourPiiId).head.pii.called
      val assignedIds: Seq[Int] = fstMsgs[Assignment].filter(_.pii.id == ourPiiId).map(_.callRef.id)
      calledIds should contain theSameElementsAs assignedIds
    }

    checkForOutstandingMsgs(sndMsgs)
    checkForUnmatchedLogs(sndMsgs)
  }

  it should "call an Rexample (with an outstanding PiiUpdate)" in {

    // Construct and send an outstanding PiiUpdate message to test robustness.
    val oldPii: PiInstance[ObjectId] = PiInstance.forCall(ObjectId.get, ri, 2, 1)

    val msgsOf: MessageMap = {
      val ex = makeExecutor(completeProcess.settings)

      tryBut {
        KafkaConnectors.sendMessages(PiiUpdate(oldPii))(completeProcess.settings)

        val f1 = ex.execute(ri, Seq(21))
        await(f1) should be(("PbISleptFor2s", "PcISleptFor1s"))

      } always {
        ensureShutdownThen(ex) {
          // We don't care what state we leave the outstanding message in, provided we clean our own state.
          val ourMsg: AnyMsg => Boolean = _.piiId != oldPii.id
          new MessageDrain(true).filter(ourMsg)
        }
      }
    }

    checkForOutstandingMsgs(msgsOf)
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
        KafkaConnectors.sendMessages(oldMsg)(completeProcess.settings)

        val f1 = ex.execute(ri, Seq(21))
        await(f1) should be(("PbISleptFor2s", "PcISleptFor1s"))

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    def shouldBeEmpty(m: AnyMsg): Boolean = !m.isInstanceOf[PiiUpdate]
    checkForOutstandingMsgs(msgsOf filter shouldBeEmpty)

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

          await(f0) shouldBe(s"PbISleptFor${b(0)}s", s"PcISleptFor${b(1)}s")
          await(f1) shouldBe(s"PbISleptFor${b(2)}s", s"PcISleptFor${b(3)}s")
          await(f2) shouldBe(s"PbISleptFor${b(4)}s", s"PcISleptFor${b(5)}s")
        }

      } always {
        ensureShutdownThen(ex) {
          new MessageDrain(true)
        }
      }
    }

    checkForOutstandingMsgs(msgsOf)
    checkForUnmatchedLogs(msgsOf)
  }
}
