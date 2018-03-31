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

@RunWith(classOf[JUnitRunner])
class MongoExecutorTests extends FlatSpec with Matchers with ProcessExecutorTester {
  implicit val system: ActorSystem = ActorSystem("MongoExecutorTests")
  implicit val executionContext = system.dispatchers.lookup("akka.my-dispatcher")  
  
  val pai = new PaI
  val pbi = new PbI
  val pci = new PcI
  val ri = new R(pai,pbi,pci)

  it should "execute atomic PbI once" in {
    val client = MongoClient()
    val ex = new MongoDBExecutor(client, "pew", "test_exec_insts",pai,pbi,pci,ri)
    val f1 = ex.execute(pbi,Seq(2))
   
    val r1 = await(f1)
    client.close()
    r1 should not be empty
    r1 should be (Some("PbSleptFor2s"))
	}

  it should "execute atomic PbI twice concurrently" in {
    val client = MongoClient()
    val ex = new MongoDBExecutor(client, "pew", "test_exec_insts",pai,pbi,pci,ri)
    val f1 = ex.execute(pbi,Seq(2))
    val f2 = ex.execute(pbi,Seq(1))
   
    val r1 = await(f1)
    r1 should not be empty
    r1 should be (Some("PbSleptFor2s"))
    val r2 = await(f2)
    r2 should not be empty
    r2 should be (Some("PbSleptFor1s"))
    client.close()  
	}
  
  it should "execute Rexample once" in {
    val client = MongoClient()
    val ex = new MongoDBExecutor(client, "pew", "test_exec_insts",pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(21))
   
    val r1 = await(f1)
    client.close()
    r1 should be (Some(("PbSleptFor2s","PcSleptFor1s")))
	}

  it should "execute Rexample once with same timings" in {
    val client = MongoClient()
    val ex = new MongoDBExecutor(client, "pew", "test_exec_insts",pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(11))
   
    val r1 = await(f1)
    client.close()
    r1 should be (Some(("PbSleptFor1s","PcSleptFor1s")))
	}
 
  it should "execute Rexample twice concurrently" in {
    val client = MongoClient()
    val ex = new MongoDBExecutor(client, "pew", "test_exec_insts",pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(31))
    val f2 = ex.execute(ri,Seq(12))
    
    val r1 = await(f1)
    r1 should be (Some(("PbSleptFor3s","PcSleptFor1s")))
    val r2 = await(f2)
    r2 should be (Some(("PbSleptFor1s","PcSleptFor2s")))
    client.close()    
	}

  it should "execute Rexample twice with same timings concurrently" in {
    val client = MongoClient()
    val ex = new MongoDBExecutor(client, "pew", "test_exec_insts",pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(11))
    val f2 = ex.execute(ri,Seq(11))
    
    val r1 = await(f1)
    r1 should be (Some(("PbSleptFor1s","PcSleptFor1s")))
    val r2 = await(f2)
    r2 should be (Some(("PbSleptFor1s","PcSleptFor1s")))
    client.close()  
	}
  
  it should "execute Rexample thrice concurrently" in {
    val client = MongoClient()
    val ex = new MongoDBExecutor(client, "pew", "test_exec_insts",pai,pbi,pci,ri)
    val f1 = ex.execute(ri,Seq(11))
    val f2 = ex.execute(ri,Seq(11))
    val f3 = ex.execute(ri,Seq(11))
    
    val r1 = await(f1)
    r1 should be (Some(("PbSleptFor1s","PcSleptFor1s")))
    val r2 = await(f2)
    r2 should be (Some(("PbSleptFor1s","PcSleptFor1s")))
    val r3 = await(f3)
    r3 should be (Some(("PbSleptFor1s","PcSleptFor1s")))
    client.close()  
	}
}

