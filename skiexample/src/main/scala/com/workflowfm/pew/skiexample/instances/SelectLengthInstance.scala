package com.workflowfm.pew.skiexample.instances

import scala.concurrent._

import com.workflowfm.pew.skiexample.SkiExampleTypes._
import com.workflowfm.pew.skiexample.processes._

class SelectLengthInstance extends SelectLength {

  override def apply(arg0: HeightCM, arg1: WeightKG): Future[LengthCM] = {
    Future.successful("{" + arg0 + "-" + arg1 + "}>CM")
  }
}
