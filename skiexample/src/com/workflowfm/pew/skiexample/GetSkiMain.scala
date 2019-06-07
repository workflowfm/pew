package com.workflowfm.pew.skiexample

import scala.concurrent._
import scala.concurrent.duration._
import com.workflowfm.pew._
import com.workflowfm.pew.execution._
import com.workflowfm.pew.skiexample.SkiExampleTypes.{Exception, PriceNOK, _}
import com.workflowfm.pew.skiexample.processes._
import com.workflowfm.pew.skiexample.instances._
import org.mongodb.scala.MongoClient
import com.workflowfm.pew.mongodb.MongoExecutor
import akka.actor.ActorSystem
import com.workflowfm.pew.stateless.instances.kafka.CompleteKafkaExecutor
import com.workflowfm.pew.stateless.instances.kafka.settings.KafkaExecutorSettings
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.{BsonKafkaExecutorSettings, KafkaCodecRegistry}
import org.bson.codecs.configuration.CodecRegistry

object GetSkiMain {

  type Output = Either[PriceNOK,Exception]
  
  def main(args: Array[String]): Unit = {
	val selectModel = new SelectModelInstance
	val selectLength = new SelectLengthInstance
	val cM2Inch = new CM2InchInstance
	val uSD2NOK = new USD2NOKInstance
	val selectSki = new SelectSkiInstance
	
	
	val getSki = new GetSki(cM2Inch , selectLength , selectModel , selectSki , uSD2NOK)
	
	implicit val system: ActorSystem = ActorSystem("GetSkiMain")
    implicit val executionContext: ExecutionContext = ExecutionContext.global

    val procs: Seq[PiProcess] = Seq( selectModel, selectLength, cM2Inch, uSD2NOK, selectSki, getSki )

   	//implicit val executor:ProcessExecutor[_] = new MultiStateExecutor(procs:_*)
	implicit val executor:AkkaExecutor =  new AkkaExecutor(procs:_*)

	//val client = MongoClient()
    //implicit val executor = new MongoDBExecutor(client, "pew", "test_exec_insts", selectModel, selectLength, cM2Inch, uSD2NOK, selectSki, getSki)

	//implicit val executor: ProcessExecutor[_] = {

	//  val piStore: PiProcessStore = SimpleProcessStore( procs flatMap (p => p +: p.allDependencies): _*  )
	//  val fullRegistry: CodecRegistry = new KafkaCodecRegistry( piStore )

	//  implicit val settings: KafkaExecutorSettings
	//    = new BsonKafkaExecutorSettings( fullRegistry )

	//  FutureExecutorAdapter( CompleteKafkaExecutor[Output] )
	//}


	
	val fs1 = 1 to 10 map { x => (x, getSki( "height" , "price" , "skill" , "weight" )) }
    val fs2 = 1 to 10 map { x => (x, getSki( "h" , "p" , "s" , "w" )) } // small strings produce EXCEPTION in the output

    // all 20 workflows are running concurrently at this point!



	try {
	  for ((i,f) <- fs1)
		println( s"*** Result A$i: '${Await.result(f,10.seconds)}'.")
	  for ((i,f) <- fs2)
		println( s"*** Result B$i: '${Await.result(f,10.seconds)}'.")
	} catch {
	  case e:Throwable => e.printStackTrace()
	}
	
    //		val f1 = getSki( "height" , "price" , "skill" , "weight" )
    //		val f2 = getSki( "h" , "p" , "s" , "w" )
    //
    //		try {
    //		  println("*** Result 1: " + Await.result(f1,10.seconds))
    //		  println("*** Result 2: " + Await.result(f2,10.seconds))
    //		} catch {
    //		  case e:Throwable => e.printStackTrace()
    //		}
    //
	
	//client.close()
	Await.result(system.terminate(),10.seconds)
  }
}
