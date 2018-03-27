package com.workflowfm.pew.skiexample.processes

import scala.concurrent._
import com.workflowfm.pew._
import com.workflowfm.pew.skiexample.SkiExampleTypes._

trait USD2NOK extends ((PriceUSD) => Future[PriceNOK]) with AtomicProcess {
	override val name = "USD2NOK"
	override val output = (Chan("USD2NOK__a_PriceNOK"),"oUSD2NOK_PriceNOK_")
	override val inputs = Seq((Chan("USD2NOK__a_PriceUSD"),"cUSD2NOK_PriceUSD_1"))
	override val channels = Seq("cUSD2NOK_PriceUSD_1","oUSD2NOK_PriceNOK_")

	def run(args:Seq[PiObject])(implicit ec:ExecutionContext):Future[PiObject] = args match {
		case Seq(o1) => this(PiObject.getAs[PriceUSD](o1)) map PiObject.apply
	}
}
