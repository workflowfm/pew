package src.com.workflowfm.pew.skiexample.instances

import scala.concurrent._
import com.workflowfm.pew.skiexample.SkiExampleTypes._
import com.workflowfm.pew.skiexample.processes._

class USD2NOKInstance extends USD2NOK {
	override def apply( arg0 :PriceUSD ) :Future[PriceNOK] = {
		Future.successful(arg0 + ">NOK")
	}
}
