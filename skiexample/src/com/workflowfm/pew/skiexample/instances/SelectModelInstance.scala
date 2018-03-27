package src.com.workflowfm.pew.skiexample.instances

import scala.concurrent._
import com.workflowfm.pew.skiexample.SkiExampleTypes._
import com.workflowfm.pew.skiexample.processes._

class SelectModelInstance extends SelectModel {
	override def apply( arg0 :PriceLimit, arg1 :SkillLevel ) :Future[(Brand,Model)] = {
		Future.successful(("("+arg0+","+arg1+")>Brand","("+arg0+","+arg1+")>Model"))
	}
}
