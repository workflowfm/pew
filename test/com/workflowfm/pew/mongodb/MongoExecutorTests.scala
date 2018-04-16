package com.workflowfm.pew.mongodb

import akka.actor.ActorSystem
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import com.workflowfm.pew._
import com.workflowfm.pew.mongodb._
import org.mongodb.scala.MongoClient
import com.workflowfm.pew.execution._
import RexampleTypes._
import org.bson.types.ObjectId
import scala.concurrent.Promise
import scala.util.Success
import scala.util.Failure
import scala.concurrent.duration._
import org.scalatest.BeforeAndAfterAll

@RunWith(classOf[JUnitRunner])
class MongoExecutorTests extends FlatSpec with Matchers with BeforeAndAfterAll with ProcessExecutorTester {
  implicit val system: ActorSystem = ActorSystem("MongoExecutorTests")
  //implicit val executionContext = system.dispatchers.lookup("akka.my-dispatcher")  
  
  val pai = new PaI
  val pbi = new PbI
  val pci = new PcI
  val pci2 = new PcI("PcX")
  val ri = new R(pai,pbi,pci)
  val ri2 = new R(pai,pbi,pci2)
  val badri = new R(pai,pbi,pci)
  
  val client = MongoClient()
  override def afterAll:Unit = {
    client.close()
  }
  
  it should "execute atomic PbI once" in {  
    val ex = MongoFutureExecutor(client, "pew", "test_exec_insts",pai,pbi,pci,ri)
    val f1 = ex.execute(pbi,Seq(2))
   
    val r1 = awaitf(f1)
    r1 should be ("PbISleptFor2s")
	}

  it should "execute atomic PbI twice concurrently" in {
    val ex = MongoFutureExecutor(client, "pew", "test_exec_insts",pai,pbi,pci,ri)
    val f1 = ex.execute(pbi,Seq(2))
    val f2 = ex.execute(pbi,Seq(1))
   
    val r1 = awaitf(f1)
    r1 should be ("PbISleptFor2s")
    val r2 = awaitf(f2)
    r2 should be ("PbISleptFor1s")
	}
  
  it should "execute Rexample once" in {
    val ex = MongoFutureExecutor(client, "pew", "test_exec_insts",pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(21))
   
    val r1 = awaitf(f1)
    r1 should be (("PbISleptFor2s","PcISleptFor1s"))
	}

  it should "execute Rexample once with same timings" in {
    val ex = MongoFutureExecutor(client, "pew", "test_exec_insts",pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(11))
   
    val r1 = awaitf(f1)
    r1 should be (("PbISleptFor1s","PcISleptFor1s"))
	}
 
  it should "execute Rexample twice concurrently" in {
    val ex = MongoFutureExecutor(client, "pew", "test_exec_insts",pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(31))
    val f2 = ex.execute(ri,Seq(12))
    
    val r1 = awaitf(f1)
    r1 should be (("PbISleptFor3s","PcISleptFor1s"))
    val r2 = awaitf(f2)
    r2 should be (("PbISleptFor1s","PcISleptFor2s")) 
	}

  it should "execute Rexample twice with same timings concurrently" in {
    val ex = MongoFutureExecutor(client, "pew", "test_exec_insts",pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(11))
    val f2 = ex.execute(ri,Seq(11))
    
    val r1 = awaitf(f1)
    r1 should be (("PbISleptFor1s","PcISleptFor1s"))
    val r2 = awaitf(f2)
    r2 should be (("PbISleptFor1s","PcISleptFor1s"))
	}
  
  it should "execute Rexample thrice concurrently" in {
    val ex = MongoFutureExecutor(client, "pew", "test_exec_insts",pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(11))
    val f2 = ex.execute(ri,Seq(11))
    val f3 = ex.execute(ri,Seq(11))
    
    val r1 = awaitf(f1)
    r1 should be (("PbISleptFor1s","PcISleptFor1s"))
    val r2 = awaitf(f2)
    r2 should be (("PbISleptFor1s","PcISleptFor1s"))
    val r3 = awaitf(f3)
    r3 should be (("PbISleptFor1s","PcISleptFor1s")) 
	}
  
  it should "execute Rexample twice, each with a differnt component" in {
    val ex = MongoFutureExecutor(client, "pew", "test_exec_insts",pai,pbi,pci,pci2,ri,ri2)
    val f1 = ex.execute(ri,Seq(11))
    val f2 = ex.execute(ri2,Seq(11))
    
    val r1 = awaitf(f1)
    r1 should be (("PbISleptFor1s","PcISleptFor1s"))
    val r2 = awaitf(f2)
    r2 should be (("PbISleptFor1s","PcXSleptFor1s"))
	}
  
  it should "fail properly when a workflow doesn't exist" in {
    val ex = MongoFutureExecutor(client, "pew", "test_exec_insts",pai,pbi,pci,pci2,ri,ri2)
    val f1 = ex.postResult(new ObjectId(), 0, PiObject(0))
    
    a [ProcessExecutor.NoSuchInstanceException] should be thrownBy await(f1)
	}
  
  it should "handle a failing atomic process" in {
    val p = new FailP
    val ex = MongoFutureExecutor(client, "pew", "test_exec_insts",p)
    val f1 = ex.execute(p,Seq(1))
    
    try {
      await(f1)
    } catch {
      case (e:Exception) => e.getMessage should be ("FailP")
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

