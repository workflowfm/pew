package com.workflowfm.pew.mongodb
import scala.concurrent.Promise

import akka.actor.ActorSystem
import org.bson.types.ObjectId
import org.junit.runner.RunWith
import org.mongodb.scala.MongoClient
import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers }
import org.scalatest.junit.JUnitRunner

import com.workflowfm.pew._
import com.workflowfm.pew.execution._
import com.workflowfm.pew.execution.RexampleTypes._
import com.workflowfm.pew.stream._

@RunWith(classOf[JUnitRunner])
class MongoExecutorTests
    extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with ProcessExecutorTester {
  implicit val system: ActorSystem = ActorSystem("MongoExecutorTests")
  //implicit val executionContext = system.dispatchers.lookup("akka.my-dispatcher")

  val pai = new PaI
  val pbi = new PbI
  val pci = new PcI
  val pci2 = new PcI("PcX")
  val ri = new R(pai, pbi, pci)
  val ri2 = new R(pai, pbi, pci2)
  val badri = new R(pai, pbi, pci)

  val client: MongoClient = MongoClient()

  override def afterAll: Unit = {
    client.close()
  }

  it should "execute atomic PbI once" in {
    val ex = new MongoExecutor(client, "pew", "test_exec_insts", pai, pbi, pci, ri)
    val f1 = ex.execute(pbi, Seq(2))

    val r1 = await(f1)
    r1 should be("PbISleptFor2s")
  }

  it should "execute atomic PbI twice concurrently" in {
    val ex = new MongoExecutor(client, "pew", "test_exec_insts", pai, pbi, pci, ri)
    val f1 = ex.execute(pbi, Seq(2))
    val f2 = ex.execute(pbi, Seq(1))

    val r1 = await(f1)
    r1 should be("PbISleptFor2s")
    val r2 = await(f2)
    r2 should be("PbISleptFor1s")
  }

  it should "execute Rexample once" in {
    val ex = new MongoExecutor(client, "pew", "test_exec_insts", pai, pbi, pci, ri)
    val f1 = ex.execute(ri, Seq(21))

    val r1 = await(f1)
    r1 should be(("PbISleptFor2s", "PcISleptFor1s"))
  }

  it should "execute Rexample once with same timings" in {
    val ex = new MongoExecutor(client, "pew", "test_exec_insts", pai, pbi, pci, ri)
    val f1 = ex.execute(ri, Seq(11))

    val r1 = await(f1)
    r1 should be(("PbISleptFor1s", "PcISleptFor1s"))
  }

  it should "execute Rexample twice concurrently" in {
    val ex = new MongoExecutor(client, "pew", "test_exec_insts", pai, pbi, pci, ri)
    val f1 = ex.execute(ri, Seq(31))
    val f2 = ex.execute(ri, Seq(12))

    val r1 = await(f1)
    r1 should be(("PbISleptFor3s", "PcISleptFor1s"))
    val r2 = await(f2)
    r2 should be(("PbISleptFor1s", "PcISleptFor2s"))
  }

  it should "execute Rexample twice with same timings concurrently" in {
    val ex = new MongoExecutor(client, "pew", "test_exec_insts", pai, pbi, pci, ri)
    val f1 = ex.execute(ri, Seq(11))
    val f2 = ex.execute(ri, Seq(11))

    val r1 = await(f1)
    r1 should be(("PbISleptFor1s", "PcISleptFor1s"))
    val r2 = await(f2)
    r2 should be(("PbISleptFor1s", "PcISleptFor1s"))
  }

  it should "execute Rexample thrice concurrently" in {
    val ex = new MongoExecutor(client, "pew", "test_exec_insts", pai, pbi, pci, ri)
    val f1 = ex.execute(ri, Seq(11))
    val f2 = ex.execute(ri, Seq(11))
    val f3 = ex.execute(ri, Seq(11))

    val r1 = await(f1)
    r1 should be(("PbISleptFor1s", "PcISleptFor1s"))
    val r2 = await(f2)
    r2 should be(("PbISleptFor1s", "PcISleptFor1s"))
    val r3 = await(f3)
    r3 should be(("PbISleptFor1s", "PcISleptFor1s"))
  }

  it should "execute Rexample twice, each with a differnt component" in {
    val ex = new MongoExecutor(client, "pew", "test_exec_insts", pai, pbi, pci, pci2, ri, ri2)
    val f1 = ex.execute(ri, Seq(11))
    val f2 = ex.execute(ri2, Seq(11))

    val r1 = await(f1)
    r1 should be(("PbISleptFor1s", "PcISleptFor1s"))
    val r2 = await(f2)
    r2 should be(("PbISleptFor1s", "PcXSleptFor1s"))
  }

  it should "fail properly when a workflow doesn't exist" in {
    val ex = new MongoExecutor(client, "pew", "test_exec_insts", pai, pbi, pci, pci2, ri, ri2)
    val id = new ObjectId()
    val handler = new ResultHandler(id)
    ex.subscribe(handler)
    ex.postResult(id, 0, MetadataAtomicProcess.result(PiItem(0)))

    a[NoSuchInstanceException[ObjectId]] should be thrownBy await(handler.future)
  }

  it should "handle a failing atomic process" in {
    val p = new FailP
    val ex = new MongoExecutor(client, "pew", "test_exec_insts", p)
    val f1 = ex.execute(p, Seq(1))

    try {
      await(f1)
    } catch {
      case (e: Exception) => e.getMessage should be("FailP")
    }
  }

//  it should "fail properly when a process doesn't exist" in {
//    val ex = MongoFutureExecutor(client, "pew", "test_exec_insts",pai,pci,pci2,badri)
//    val f1 = ex.execute(ri,Seq(11))
//
//    val a1 = await(f1)
//    println(a1)
//    val a2 = await(a1)
//    println(a2)
//    a [ProcessExecutor.NoSuchInstanceException] should be thrownBy await(f1)
//	}
}
