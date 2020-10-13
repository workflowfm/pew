package com.workflowfm.pew.skiexample

import java.util.UUID

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Try

import akka.actor.ActorSystem
//import org.bson.codecs.configuration.CodecRegistry
//import org.mongodb.scala.MongoClient

import com.workflowfm.pew._
import com.workflowfm.pew.execution._
import com.workflowfm.pew.metrics.MetricsD3Timeline
import com.workflowfm.pew.metrics.MetricsHandler
//import com.workflowfm.pew.mongodb.MongoExecutor
import com.workflowfm.pew.skiexample.SkiExampleTypes._
import com.workflowfm.pew.skiexample.instances._
import com.workflowfm.pew.skiexample.processes._
//import com.workflowfm.pew.kafka.CompleteKafkaExecutor
//import com.workflowfm.pew.kafka.settings.KafkaExecutorSettings
//import com.workflowfm.pew.kafka.settings.bson.{
//  BsonKafkaExecutorSettings,
//  KafkaCodecRegistry
//}
import com.workflowfm.pew.stream.PrintEventHandler

object GetSkiMain {

  type Output = Either[PriceNOK, Exception]

  def main(args: Array[String]): Unit = {
    val selectModel = new SelectModelInstance
    val selectLength = new SelectLengthInstance
    val cM2Inch = new CM2InchInstance
    val uSD2NOK = new USD2NOKInstance
    val selectSki = new SelectSkiInstance

    val getSki = new GetSki(cM2Inch, selectLength, selectModel, selectSki, uSD2NOK)

    implicit val system: ActorSystem = ActorSystem("GetSkiMain")
    implicit val executionContext: ExecutionContext = ExecutionContext.global

    val procs: Seq[PiProcess] = Seq(selectModel, selectLength, cM2Inch, uSD2NOK, selectSki, getSki)

    //implicit val executor:ProcessExecutor[_] = new MultiStateExecutor(procs:_*)
    implicit val executor: AkkaExecutor = new AkkaExecutor()

    val handler = new MetricsHandler[UUID]
    val switch = Await.result(executor.subscribe(handler), 3.seconds)
    //Await.result(executor.subscribe(new PrintEventHandler), 3.seconds)

    //val client = MongoClient()
    /* implicit val executor = new MongoDBExecutor(client, "pew", "test_exec_insts", selectModel,
     * selectLength, cM2Inch, uSD2NOK, selectSki, getSki) */

    //implicit val executor: ProcessExecutor[_] = {

    /* val piStore: PiProcessStore = SimpleProcessStore( procs flatMap (p => p +:
     * p.allDependencies): _* ) */
    //  val fullRegistry: CodecRegistry = new KafkaCodecRegistry( piStore )

    //  implicit val settings: KafkaExecutorSettings
    //    = new BsonKafkaExecutorSettings( fullRegistry )

    //  FutureExecutorAdapter( CompleteKafkaExecutor[Output] )
    //}

    def tryToInt(s: String) = Try(s.toInt).toOption
    val n = if (args.size > 0) {
      tryToInt(args(0)).getOrElse(5)
    } else 5

    val fs1 = 1 to n map { x => (x, getSki("height", "price", "skill", "weight")) }
    val fs2 = 1 to n map { x => (x, getSki("h", "p", "s", "w")) } // small strings produce EXCEPTION in the output

    // all 20 workflows are running concurrently at this point!

    try {
      for ((i, f) <- fs1)
        println(s"*** Result A$i: '${Await.result(f, 10.seconds)}'.")
      for ((i, f) <- fs2)
        println(s"*** Result B$i: '${Await.result(f, 10.seconds)}'.")
    } catch {
      case e: Throwable => e.printStackTrace()
    }

    switch.stop
    new MetricsD3Timeline[UUID]("skiexample/output/", "ski")(handler)

    //client.close()
    Await.result(system.terminate(), 10.seconds)
  }
}
