package com.workflowfm.pew.skiexample

import scala.concurrent._
import scala.concurrent.duration.Duration
import com.workflowfm.pew._
import com.workflowfm.pew.execution._
import com.workflowfm.pew.skiexample.SkiExampleTypes._
import com.workflowfm.pew.skiexample.processes._
import com.workflowfm.pew.skiexample.instances._

object GetSkiMain {
	
  def main(args: Array[String]): Unit = {
		val selectModel = new SelectModelInstance
		val selectLength = new SelectLengthInstance
		val cM2Inch = new CM2InchInstance
		val uSD2NOK = new USD2NOKInstance
		val selectSki = new SelectSkiInstance
		
		
		val getSki = new GetSki(cM2Inch , selectLength , selectModel , selectSki , uSD2NOK)
		
		//implicit val executor:ProcessExecutor = SingleBlockingExecutor()
		//println("*** Result 1: " + Await.result(getSki( "height" , "price" , "skill" , "weight" ),Duration.Inf))
		//println("*** Result 2: " + Await.result(getSki( "h" , "p" , "s" , "w" ),Duration.Inf))

   	implicit val executor:ProcessExecutor = new MultiStateExecutor(selectModel, selectLength, cM2Inch, uSD2NOK, selectSki, getSki)
    val f1 = getSki( "height" , "price" , "skill" , "weight" )
		val f2 = getSki( "h" , "p" , "s" , "w" )
		
    println("*** Result 1: " + Await.result(f1,Duration.Inf))
		println("*** Result 2: " + Await.result(f2,Duration.Inf))
	}
}
