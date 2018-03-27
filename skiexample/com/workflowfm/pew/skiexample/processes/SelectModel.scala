package com.workflowfm.pew.skiexample.processes

import scala.concurrent._
import com.workflowfm.pew._
import com.workflowfm.pew.skiexample.SkiExampleTypes._

trait SelectModel extends ((PriceLimit, SkillLevel) => Future[(Brand,Model)]) with AtomicProcess {
	override val name = "SelectModel"
	override val output = (PiPair(Chan("SelectModel_l_a_Brand"),Chan("SelectModel_r_a_Model")),"oSelectModel_lB_Brand_x_Model_rB_")
	override val inputs = Seq((Chan("SelectModel__a_PriceLimit"),"cSelectModel_PriceLimit_1"),(Chan("SelectModel__a_SkillLevel"),"cSelectModel_SkillLevel_2"))
	override val channels = Seq("cSelectModel_PriceLimit_1","cSelectModel_SkillLevel_2","oSelectModel_lB_Brand_x_Model_rB_")

	def run(args:Seq[PiObject])(implicit ec:ExecutionContext):Future[PiObject] = args match {
		case Seq(o1,o2) => this(PiObject.getAs[PriceLimit](o1),PiObject.getAs[SkillLevel](o2)) map PiObject.apply
	}
}

