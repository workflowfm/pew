package com.workflowfm.pew.skiexample.processes

import scala.concurrent._
import com.workflowfm.pew._
import com.workflowfm.pew.execution._
import com.workflowfm.pew.skiexample.SkiExampleTypes._

class GetSki(cM2Inch:CM2Inch,selectLength:SelectLength,selectModel:SelectModel,selectSki:SelectSki,uSD2NOK:USD2NOK) extends CompositeProcess { // (HeightCM, PriceLimit, SkillLevel, WeightKG) => Future[Either[PriceNOK,Exception]]
	override val name = "GetSki"
	override val output = (PiOpt(Chan("GetSki_l_a_PriceNOK"),Chan("GetSki_r_a_Exception")),"y12")
	override val inputs = Seq((Chan("GetSki__a_HeightCM"),"cSelectLength_HeightCM_1"),(Chan("GetSki__a_PriceLimit"),"cSelectModel_PriceLimit_1"),(Chan("GetSki__a_SkillLevel"),"cSelectModel_SkillLevel_2"),(Chan("GetSki__a_WeightKG"),"cSelectLength_WeightKG_2"))
	override val channels = Seq("cSelectLength_HeightCM_1","cSelectModel_PriceLimit_1","cSelectModel_SkillLevel_2","cSelectLength_WeightKG_2","y12")

	override val dependencies = Seq(cM2Inch,selectLength,selectModel,selectSki,uSD2NOK)

	override val body = PiCut("z14","x12","oSelectSki_lB_PriceUSD_Plus_Exception_rB_",WithIn("x12","cUSD2NOK_PriceUSD_1","c12",LeftOut("y12","oUSD2NOK_PriceNOK_",PiCall<("USD2NOK","cUSD2NOK_PriceUSD_1","oUSD2NOK_PriceNOK_")),RightOut("y12","d12",PiId("c12","d12","m13"))),PiCut("z9","z8","oSelectModel_lB_Brand_x_Model_rB_",ParInI("z8","cSelectSki_Brand_2","cSelectSki_Model_3",PiCut("z4","cSelectSki_LengthInch_1","oCM2Inch_LengthInch_",PiCall<("SelectSki","cSelectSki_LengthInch_1","cSelectSki_Brand_2","cSelectSki_Model_3","oSelectSki_lB_PriceUSD_Plus_Exception_rB_"),PiCut("z2","cCM2Inch_LengthCM_1","oSelectLength_LengthCM_",PiCall<("CM2Inch","cCM2Inch_LengthCM_1","oCM2Inch_LengthInch_"),PiCall<("SelectLength","cSelectLength_HeightCM_1","cSelectLength_WeightKG_2","oSelectLength_LengthCM_")))),PiCall<("SelectModel","cSelectModel_PriceLimit_1","cSelectModel_SkillLevel_2","oSelectModel_lB_Brand_x_Model_rB_")))
	
	def apply(heightCM:HeightCM,priceLimit:PriceLimit,skillLevel:SkillLevel,weightKG:WeightKG)(implicit executor:ProcessExecutor): Future[Option[Either[PriceNOK,Exception]]] =
		executor.execute(this,Seq(heightCM,priceLimit,skillLevel,weightKG)).map(_ map(_.asInstanceOf[Either[PriceNOK,Exception]]))(executor.context)
}
