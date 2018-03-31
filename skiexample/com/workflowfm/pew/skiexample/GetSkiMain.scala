package com.workflowfm.pew.skiexample

import scala.concurrent._
import scala.concurrent.duration._
import com.workflowfm.pew._
import com.workflowfm.pew.execution._
import com.workflowfm.pew.skiexample.SkiExampleTypes._
import com.workflowfm.pew.skiexample.processes._
import com.workflowfm.pew.skiexample.instances._
import org.mongodb.scala.MongoClient
import com.workflowfm.pew.mongodb.MongoDBExecutor
import akka.actor.ActorSystem

object GetSkiMain {
	
  def main(args: Array[String]): Unit = {
		val selectModel = new SelectModelInstance
		val selectLength = new SelectLengthInstance
		val cM2Inch = new CM2InchInstance
		val uSD2NOK = new USD2NOKInstance
		val selectSki = new SelectSkiInstance
		
		
		val getSki = new GetSki(cM2Inch , selectLength , selectModel , selectSki , uSD2NOK)
		
		implicit val system: ActorSystem = ActorSystem("GetSkiMain")
    implicit val executionContext = system.dispatchers.lookup("akka.my-dispatcher") 
		
		//implicit val executor:FutureExecutor = SingleBlockingExecutor()
   	//implicit val executor:FutureExecutor = new MultiStateExecutor(selectModel, selectLength, cM2Inch, uSD2NOK, selectSki, getSki)
		
		val client = MongoClient()
    implicit val executor = new MongoDBExecutor(client, "pew", "test_exec_insts", selectModel, selectLength, cM2Inch, uSD2NOK, selectSki, getSki)
		
    val f1 = getSki( "height" , "price" , "skill" , "weight" )
		val f2 = getSki( "h" , "p" , "s" , "w" )
	
		try {
		  println("*** Result 1: " + Await.result(f1,10.seconds))
		  println("*** Result 2: " + Await.result(f2,10.seconds))
		} catch {
		  case e:Throwable => e.printStackTrace()
		}
					
		client.close()
		Await.result(system.terminate(),10.seconds)
	}
}
