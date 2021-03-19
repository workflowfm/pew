package com.workflowfm.pew.execution

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutor

import RexampleTypes._
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MutexExecutorTests extends FlatSpec with Matchers with ProcessExecutorTester {
//implicit val system: ActorSystem = ActorSystem("MutexExecutorTests")
  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.global //system.dispatchers.lookup("akka.my-dispatcher")

  val pai = new PaI
  val pbi = new PbI
  val pci = new PcI
  val ri = new R(pai, pbi, pci)

  "MutexExecutor" should "execute atomic PbI once" in {
    val ex = new MutexExecutor()
    val f1 = ex.execute(pbi, Seq(2))

    await(f1).isSuccess should be(true)
  }

  "MutexExecutor" should "execute atomic PbI twice concurrently" in {
    val ex = new MutexExecutor()
    val f1 = ex.execute(pbi, Seq(2))
    val f2 = ex.execute(pbi, Seq(1))

    await(f1).isSuccess should be(true)
    await(f2).isSuccess should be(true)
  }

  "MutexExecutor" should "execute Rexample once" in {
    val ex = new MutexExecutor()
    val f1 = ex.execute(ri, Seq(21))

    await(f1).success.value should be(("PbISleptFor2s", "PcISleptFor1s"))
  }

  "MutexExecutor" should "execute Rexample twice concurrently" in {
    val ex = new MutexExecutor()
    val f1 = ex.execute(ri, Seq(31))
    val f2 = ex.execute(ri, Seq(12))

    await(f1).success.value should be(("PbISleptFor3s", "PcISleptFor1s"))
    await(f2).success.value should be(("PbISleptFor1s", "PcISleptFor2s"))
  }

  "MutexExecutor" should "execute Rexample twice with same timings concurrently" in {
    val ex = new MutexExecutor()
    val f1 = ex.execute(ri, Seq(11))
    val f2 = ex.execute(ri, Seq(11))

    await(f1).success.value should be(("PbISleptFor1s", "PcISleptFor1s"))
    await(f2).success.value should be(("PbISleptFor1s", "PcISleptFor1s"))
  }

  "MutexExecutor" should "execute Rexample thrice concurrently" in {
    val ex = new MutexExecutor()
    val f1 = ex.execute(ri, Seq(11))
    val f2 = ex.execute(ri, Seq(11))
    val f3 = ex.execute(ri, Seq(11))

    await(f1).success.value should be(("PbISleptFor1s", "PcISleptFor1s"))
    await(f2).success.value should be(("PbISleptFor1s", "PcISleptFor1s"))
    await(f3).success.value should be(("PbISleptFor1s", "PcISleptFor1s"))
  }

}
