package com.workflowfm.pew.skiexample.instances

import scala.concurrent._
import com.workflowfm.pew.skiexample.SkiExampleTypes._
import com.workflowfm.pew.skiexample.processes._

class CM2InchInstance extends CM2Inch {
	override def apply( arg0 :LengthCM ) :Future[LengthInch] = {
		Future.successful(arg0 + ">INCH")
	}
}
