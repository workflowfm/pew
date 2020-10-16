package com.workflowfm.pew.skiexample.instances

import scala.concurrent._

import com.workflowfm.pew.skiexample.SkiExampleTypes._
import com.workflowfm.pew.skiexample.processes._

class SelectSkiInstance extends SelectSki {

  override def apply(
      arg0: LengthInch,
      arg1: Brand,
      arg2: Model
  ): Future[Either[PriceUSD, Exception]] = {
    if (arg0.length() < 14)
      Future.successful(Right("[" + arg0 + "|" + arg1 + "|" + arg2 + "]>EXCEPTION"))
    else Future.successful(Left("[" + arg0 + "|" + arg1 + "|" + arg2 + "]>PRICE"))
  }
}
