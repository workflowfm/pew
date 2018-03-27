package src.com.workflowfm.pew.skiexample.processes

import scala.concurrent._
import com.workflowfm.pew._
import com.workflowfm.pew.skiexample.SkiExampleTypes._

trait SelectSki extends ((LengthInch, Brand, Model) => Future[Either[PriceUSD,Exception]]) with AtomicProcess {
	override val name = "SelectSki"
	override val output = (PiOpt(Chan("SelectSki_l_a_PriceUSD"),Chan("SelectSki_r_a_Exception")),"oSelectSki_lB_PriceUSD_Plus_Exception_rB_")
	override val inputs = Seq((Chan("SelectSki__a_LengthInch"),"cSelectSki_LengthInch_1"),(Chan("SelectSki__a_Brand"),"cSelectSki_Brand_2"),(Chan("SelectSki__a_Model"),"cSelectSki_Model_3"))
	override val channels = Seq("cSelectSki_LengthInch_1","cSelectSki_Brand_2","cSelectSki_Model_3","oSelectSki_lB_PriceUSD_Plus_Exception_rB_")

	def run(args:Seq[PiObject])(implicit ec:ExecutionContext):Future[PiObject] = args match {
		case Seq(o1,o2,o3) => this(PiObject.getAs[LengthInch](o1),PiObject.getAs[Brand](o2),PiObject.getAs[Model](o3)) map PiObject.apply
	}
}
