package com.workflowfm.pew.execution

import com.workflowfm.pew._
import com.workflowfm.pew.execution.CodeOutput._

import scala.concurrent.{ ExecutionContext, Future }

//@RunWith(classOf[JUnitRunner])
//class CodeOutputTests extends FlatSpec with Matchers with ProcessExecutorTester {
//  import CodeOutput._
//
//  val r1 = new R1(PaInst,PbInst)
//  val r2 = new R2(PcInst,r1)
//
//  "Default Executor" should "execute R1" in {
/* ProcessExecutor.default withProcs (PaInst,PbInst,PcInst,r1,r2) call("R1",PiItem("HI!")) should
 * be( Some("HI!>Pa>Pb") ) */
//  }
//
//  "Default Executor" should "execute R2" in {
/* ProcessExecutor.default withProcs (PaInst,PbInst,PcInst,r1,r2) call("R2",PiItem("HI!")) should
 * be( Some("HI!>Pa>Pb>Pc") ) */
//  }
//
//  val r3 = new R3(PdInst,PeInst)
//  "Default Executor" should "execute R3" in {
/* ProcessExecutor.default withProcs (PdInst,PeInst,r3) call("R3",PiItem("ab")) should be(
 * Some(Left("abA")) ) */
/* ProcessExecutor.default withProcs (PdInst,PeInst,r3) call("R3",PiItem("abc")) should be(
 * Some(Right(("abcB:L","abcC"))) ) */
//  }
//
//  val r3b = new R3b(PdInst,PeInst)
//  "Default Executor" should "execute R3b" in {
/* ProcessExecutor.default withProcs (PdInst,PeInst,r3b) call("R3b",PiItem("ab")) should be(
 * Some(Left("abA:R")) ) */
/* ProcessExecutor.default withProcs (PdInst,PeInst,r3b) call("R3b",PiItem("abc")) should be(
 * Some(Right(("abcB","abcC"))) ) */
//  }
//
//  val gs = new GetSki(CM2InchInst,SelectLengthInst,SelectModelInst,SelectSkiInst,USD2NOKInst)
/* "Default Executor" should "execute GetSki" in { // (HeightCM, PriceLimit, SkillLevel, WeightKG)
 * => Either[PriceNOK,Exception] */
/* ProcessExecutor.default withProcs (gs)
 * call("GetSki",PiItem("height"),PiItem("price"),PiItem("skill"),PiItem("weight")) should be(
 * Some(Left("USD2NOK(PriceUSD(CM2Inch(LengthCM(height,weight)),Brand(price,skill),Model(price,skill)))"))
 * ) */
/* ProcessExecutor.default withProcs (gs)
 * call("GetSki",PiItem("h"),PiItem("pl"),PiItem("sl"),PiItem("w")) should be(
 * Some(Right("EXCEPTION(CM2Inch(LengthCM(h,w)),Brand(pl,sl),Model(pl,sl))")) ) */
//  }
//
/* "SimpleProcessExecutor" should "execute GetSki through its apply function" in { // (HeightCM,
 * PriceLimit, SkillLevel, WeightKG) => Either[PriceNOK,Exception] */
//    implicit val executor = ProcessExecutor.default
/* await(gs("height","price","skill","weight")) should be(
 * Some(Left("USD2NOK(PriceUSD(CM2Inch(LengthCM(height,weight)),Brand(price,skill),Model(price,skill)))"))
 * ) */
/* await(gs("h","pl","sl","w")) should be(
 * Some(Right("EXCEPTION(CM2Inch(LengthCM(h,w)),Brand(pl,sl),Model(pl,sl))")) ) */
//  }
//
//}

object PaInst extends Pa {

  override def apply(arg0: X): (A) = {
    println("Pa got [" + arg0 + "]")
    arg0 + ">Pa"
  }
}

object PbInst extends Pb {

  override def apply(arg0: A): (Y) = {
    println("Pb got [" + arg0 + "]")
    arg0 + ">Pb"
  }
}

object PcInst extends Pc {

  override def apply(arg0: Y): (Z) = {
    println("Pc got [" + arg0 + "]")
    arg0 + ">Pc"
  }
}

object PdInst extends Pd {

  override def apply(arg0: X): (Either[A, (B, G)]) = {
    println("Pd got [" + arg0 + "]")
    if (arg0.length() < 3) Left(arg0 + "A")
    else Right((arg0 + "B", arg0 + "C"))
  }
}

object PeInst extends Pe { //((Either[B,A]) => Y)
  override def apply(arg0: Either[B, A]): (Y) = {
    println("Pe got [" + arg0 + "]")
    arg0 match {
      case Left(x) => x + ":L"
      case Right(x) => x + ":R"
    }
  }
}

object SelectModelInst extends SelectModel { //((PriceLimit, SkillLevel) => (Brand,Model))
  override def apply(arg0: PriceLimit, arg1: SkillLevel): Future[(Brand, Model)] = {
    println("SelectModel received: " + arg0 + " - " + arg1)
    Future.successful(("Brand(" + arg0 + "," + arg1 + ")", "Model(" + arg0 + "," + arg1 + ")"))
  }
}

object SelectLengthInst extends SelectLength { //((HeightCM, WeightKG) => LengthCM)
  override def apply(arg0: HeightCM, arg1: WeightKG): Future[LengthCM] = {
    println("SelectLength received: " + arg0 + " - " + arg1)
    Future.successful("LengthCM(" + arg0 + "," + arg1 + ")")
  }
}

object CM2InchInst extends CM2Inch {

  override def apply(arg0: LengthCM): Future[LengthInch] = {
    println("CM2Inch received: " + arg0)
    Future.successful("CM2Inch(" + arg0 + ")")
  }
}

object USD2NOKInst extends USD2NOK {

  override def apply(arg0: PriceUSD): Future[PriceNOK] = {
    println("USD2NOK received: " + arg0)
    Future.successful("USD2NOK(" + arg0 + ")")
  }
}

object SelectSkiInst extends SelectSki {

  override def apply(
      arg0: LengthInch,
      arg1: Brand,
      arg2: Model
  ): Future[Either[PriceUSD, Exception]] = {
    println("SelectSki received: " + arg0 + " - " + arg1 + " - " + arg2)
    if (arg0.length() > 23)
      Future.successful(Left("PriceUSD(" + arg0 + "," + arg1 + "," + arg2 + ")"))
    else Future.successful(Right("EXCEPTION(" + arg0 + "," + arg1 + "," + arg2 + ")"))
  }
}

package object CodeOutput {
  type A = String
  type B = String
  type C = String
  type G = String
  type X = String
  type Y = String
  type Z = String

  trait Pa extends ((X) => A) with AtomicProcess {
    override val name = "Pa"
    override val output = (Chan("Pa__a_A"), "oPa_A_")
    override val inputs = Seq((Chan("Pa__a_X"), "cPa_X_1"))
    override val channels = Seq("cPa_X_1", "oPa_A_")

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] = args match {
      case Seq(o1) => Future.successful(PiObject(this(PiObject.getAs[X](o1))))
    }
  }

  trait Pb extends ((A) => Y) with AtomicProcess {
    override val name = "Pb"
    override val output = (Chan("Pb__a_Y"), "oPb_Y_")
    override val inputs = Seq((Chan("Pb__a_A"), "cPb_A_1"))
    override val channels = Seq("cPb_A_1", "oPb_Y_")

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] = args match {
      case Seq(o1) => Future.successful(PiObject(this(PiObject.getAs[A](o1))))
    }
  }

  class R1(pa: Pa, pb: Pb) extends CompositeProcess { // (X) => Y
    override val name = "R1"
    override val output = (Chan("R1__a_Y"), "oPb_Y_")
    override val inputs = Seq((Chan("R1__a_X"), "cPa_X_1"))
    override val channels = Seq("cPa_X_1", "oPb_Y_")

    override val dependencies = Seq(pa, pb)

    override val body = PiCut(
      "z2",
      "cPb_A_1",
      "oPa_A_",
      PiCall < ("Pb", "cPb_A_1", "oPb_Y_"),
      PiCall < ("Pa", "cPa_X_1", "oPa_A_")
    )
  }

  trait Pc extends ((Y) => Z) with AtomicProcess {
    override val name = "Pc"
    override val output = (Chan("Pc__a_Z"), "oPc_Z_")
    override val inputs = Seq((Chan("Pc__a_Y"), "cPc_Y_1"))
    override val channels = Seq("cPc_Y_1", "oPc_Z_")

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] = args match {
      case Seq(o1) => Future.successful(PiObject(this(PiObject.getAs[Y](o1))))
    }
  }

  class R2(pc: Pc, r1: R1) extends CompositeProcess { // (X) => Z
    override val name = "R2"
    override val output = (Chan("R2__a_Z"), "oPc_Z_")
    override val inputs = Seq((Chan("R2__a_X"), "cPa_X_1"))
    override val channels = Seq("cPa_X_1", "oPc_Z_")

    override val dependencies = Seq(pc, r1)

    override val body = PiCut(
      "z2",
      "cPc_Y_1",
      "oPb_Y_",
      PiCall < ("Pc", "cPc_Y_1", "oPc_Z_"),
      PiCall < ("R1", "cPa_X_1", "oPb_Y_")
    )
  }

  trait Pd extends ((X) => Either[A, (B, G)]) with AtomicProcess {
    override val name = "Pd"

    override val output = (
      PiOpt(Chan("Pd_l_a_A"), PiPair(Chan("Pd_rl_a_B"), Chan("Pd_rr_a_G"))),
      "oPd_lB_A_Plus_lB_B_x_G_rB_rB_"
    )
    override val inputs = Seq((Chan("Pd__a_X"), "cPd_X_1"))
    override val channels = Seq("cPd_X_1", "oPd_lB_A_Plus_lB_B_x_G_rB_rB_")

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] = args match {
      case Seq(o1) => Future.successful(PiObject(this(PiObject.getAs[X](o1))))
    }
  }

  trait Pe extends ((Either[B, A]) => Y) with AtomicProcess {
    override val name = "Pe"
    override val output = (Chan("Pe__a_Y"), "oPe_Y_")
    override val inputs = Seq((PiOpt(Chan("Pe_l_a_B"), Chan("Pe_r_a_A")), "cPe_lB_B_Plus_A_rB_1"))
    override val channels = Seq("cPe_lB_B_Plus_A_rB_1", "oPe_Y_")

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] = args match {
      case Seq(o1) => Future.successful(PiObject(this(PiObject.getAs[Either[B, A]](o1))))
    }
  }

  class R3(pd: Pd, pe: Pe) extends CompositeProcess { // (X) => Either[A,(Y,G)]
    override val name = "R3"

    override val output =
      (PiOpt(Chan("R3_l_a_A"), PiPair(Chan("R3_rl_a_Y"), Chan("R3_rr_a_G"))), "y13")
    override val inputs = Seq((Chan("R3__a_X"), "cPd_X_1"))
    override val channels = Seq("cPd_X_1", "y13")

    override val dependencies = Seq(pd, pe)

    override val body = PiCut(
      "z15",
      "x13",
      "oPd_lB_A_Plus_lB_B_x_G_rB_rB_",
      WithIn(
        "x13",
        "a13",
        "z12",
        LeftOut("y13", "b13", PiId("a13", "b13", "m14")),
        RightOut(
          "y13",
          "z10",
          ParInI(
            "z12",
            "b5",
            "buf10",
            ParOut(
              "z10",
              "oPe_Y_",
              "b10",
              PiCut(
                "z5",
                "cPe_lB_B_Plus_A_rB_1",
                "cPe_lB_B_Plus_A_rB_1",
                PiCall < ("Pe", "cPe_lB_B_Plus_A_rB_1", "oPe_Y_"),
                LeftOut("cPe_lB_B_Plus_A_rB_1", "x6", PiId("b5", "x6", "m7"))
              ),
              PiId("buf10", "b10", "m11")
            )
          )
        )
      ),
      PiCall < ("Pd", "cPd_X_1", "oPd_lB_A_Plus_lB_B_x_G_rB_rB_")
    )
  }

  class R3b(pd: Pd, pe: Pe) extends CompositeProcess { // (X) => Either[Y,(B,G)]
    override val name = "R3b"

    override val output =
      (PiOpt(Chan("R3b_l_a_Y"), PiPair(Chan("R3b_rl_a_B"), Chan("R3b_rr_a_G"))), "y6")
    override val inputs = Seq((Chan("R3b__a_X"), "cPd_X_1"))
    override val channels = Seq("cPd_X_1", "y6")

    override val dependencies = Seq(pd, pe)

    override val body = PiCut(
      "z11",
      "x6",
      "oPd_lB_A_Plus_lB_B_x_G_rB_rB_",
      WithIn(
        "x6",
        "b3",
        "c6",
        LeftOut(
          "y6",
          "oPe_Y_",
          PiCut(
            "z3",
            "cPe_lB_B_Plus_A_rB_1",
            "cPe_lB_B_Plus_A_rB_1",
            PiCall < ("Pe", "cPe_lB_B_Plus_A_rB_1", "oPe_Y_"),
            RightOut("cPe_lB_B_Plus_A_rB_1", "y4", PiId("b3", "y4", "m5"))
          )
        ),
        RightOut(
          "y6",
          "d6",
          ParInI(
            "c6",
            "x7",
            "y7",
            ParOut("d6", "x8", "y8", PiId("x7", "x8", "m9"), PiId("y7", "y8", "m10"))
          )
        )
      ),
      PiCall < ("Pd", "cPd_X_1", "oPd_lB_A_Plus_lB_B_x_G_rB_rB_")
    )
  }

  type PriceLimit = String
  type SkillLevel = String
  type Brand = String
  type Model = String
  type HeightCM = String
  type WeightKG = String
  type LengthCM = String
  type LengthInch = String
  type PriceUSD = String
  type PriceNOK = String
  type Exception = String

  trait SelectModel
      extends ((PriceLimit, SkillLevel) => Future[(Brand, Model)])
      with AtomicProcess {
    override val name = "SelectModel"

    override val output = (
      PiPair(Chan("SelectModel_l_a_Brand"), Chan("SelectModel_r_a_Model")),
      "oSelectModel_lB_Brand_x_Model_rB_"
    )

    override val inputs = Seq(
      (Chan("SelectModel__a_PriceLimit"), "cSelectModel_PriceLimit_1"),
      (Chan("SelectModel__a_SkillLevel"), "cSelectModel_SkillLevel_2")
    )

    override val channels = Seq(
      "cSelectModel_PriceLimit_1",
      "cSelectModel_SkillLevel_2",
      "oSelectModel_lB_Brand_x_Model_rB_"
    )

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] = args match {
      case Seq(o1, o2) =>
        this(PiObject.getAs[PriceLimit](o1), PiObject.getAs[SkillLevel](o2)) map PiObject.apply
    }
  }

  trait SelectLength extends ((HeightCM, WeightKG) => Future[LengthCM]) with AtomicProcess {
    override val name = "SelectLength"
    override val output = (Chan("SelectLength__a_LengthCM"), "oSelectLength_LengthCM_")

    override val inputs = Seq(
      (Chan("SelectLength__a_HeightCM"), "cSelectLength_HeightCM_1"),
      (Chan("SelectLength__a_WeightKG"), "cSelectLength_WeightKG_2")
    )

    override val channels =
      Seq("cSelectLength_HeightCM_1", "cSelectLength_WeightKG_2", "oSelectLength_LengthCM_")

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] = args match {
      case Seq(o1, o2) =>
        this(PiObject.getAs[HeightCM](o1), PiObject.getAs[WeightKG](o2)) map PiObject.apply
    }
  }

  trait CM2Inch extends ((LengthCM) => Future[LengthInch]) with AtomicProcess {
    override val name = "CM2Inch"
    override val output = (Chan("CM2Inch__a_LengthInch"), "oCM2Inch_LengthInch_")
    override val inputs = Seq((Chan("CM2Inch__a_LengthCM"), "cCM2Inch_LengthCM_1"))
    override val channels = Seq("cCM2Inch_LengthCM_1", "oCM2Inch_LengthInch_")

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] = args match {
      case Seq(o1) => this(PiObject.getAs[LengthCM](o1)) map PiObject.apply
    }
  }

  trait USD2NOK extends ((PriceUSD) => Future[PriceNOK]) with AtomicProcess {
    override val name = "USD2NOK"
    override val output = (Chan("USD2NOK__a_PriceNOK"), "oUSD2NOK_PriceNOK_")
    override val inputs = Seq((Chan("USD2NOK__a_PriceUSD"), "cUSD2NOK_PriceUSD_1"))
    override val channels = Seq("cUSD2NOK_PriceUSD_1", "oUSD2NOK_PriceNOK_")

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] = args match {
      case Seq(o1) => this(PiObject.getAs[PriceUSD](o1)) map PiObject.apply
    }
  }

  trait SelectSki
      extends ((LengthInch, Brand, Model) => Future[Either[PriceUSD, Exception]])
      with AtomicProcess {
    override val name = "SelectSki"

    override val output = (
      PiOpt(Chan("SelectSki_l_a_PriceUSD"), Chan("SelectSki_r_a_Exception")),
      "oSelectSki_lB_PriceUSD_Plus_Exception_rB_"
    )

    override val inputs = Seq(
      (Chan("SelectSki__a_LengthInch"), "cSelectSki_LengthInch_1"),
      (Chan("SelectSki__a_Brand"), "cSelectSki_Brand_2"),
      (Chan("SelectSki__a_Model"), "cSelectSki_Model_3")
    )

    override val channels = Seq(
      "cSelectSki_LengthInch_1",
      "cSelectSki_Brand_2",
      "cSelectSki_Model_3",
      "oSelectSki_lB_PriceUSD_Plus_Exception_rB_"
    )

    def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] = args match {
      case Seq(o1, o2, o3) =>
        this(
          PiObject.getAs[LengthInch](o1),
          PiObject.getAs[Brand](o2),
          PiObject.getAs[Model](o3)
        ) map PiObject.apply
    }
  }

  class GetSki(
      cM2Inch: CM2Inch,
      selectLength: SelectLength,
      selectModel: SelectModel,
      selectSki: SelectSki,
      uSD2NOK: USD2NOK
  ) extends CompositeProcess { // (HeightCM, PriceLimit, SkillLevel, WeightKG) => Either[PriceNOK,Exception]
    override val name = "GetSki"
    override val output = (PiOpt(Chan("GetSki_l_a_PriceNOK"), Chan("GetSki_r_a_Exception")), "y12")

    override val inputs = Seq(
      (Chan("GetSki__a_HeightCM"), "cSelectLength_HeightCM_1"),
      (Chan("GetSki__a_PriceLimit"), "cSelectModel_PriceLimit_1"),
      (Chan("GetSki__a_SkillLevel"), "cSelectModel_SkillLevel_2"),
      (Chan("GetSki__a_WeightKG"), "cSelectLength_WeightKG_2")
    )

    override val channels = Seq(
      "cSelectLength_HeightCM_1",
      "cSelectModel_PriceLimit_1",
      "cSelectModel_SkillLevel_2",
      "cSelectLength_WeightKG_2",
      "y12"
    )

    override val dependencies = Seq(cM2Inch, selectLength, selectModel, selectSki, uSD2NOK)

    override val body = PiCut(
      "z14",
      "x12",
      "oSelectSki_lB_PriceUSD_Plus_Exception_rB_",
      WithIn(
        "x12",
        "cUSD2NOK_PriceUSD_1",
        "c12",
        LeftOut(
          "y12",
          "oUSD2NOK_PriceNOK_",
          PiCall < ("USD2NOK", "cUSD2NOK_PriceUSD_1", "oUSD2NOK_PriceNOK_")
        ),
        RightOut("y12", "d12", PiId("c12", "d12", "m13"))
      ),
      PiCut(
        "z9",
        "z8",
        "oSelectModel_lB_Brand_x_Model_rB_",
        ParInI(
          "z8",
          "cSelectSki_Brand_2",
          "cSelectSki_Model_3",
          PiCut(
            "z4",
            "cSelectSki_LengthInch_1",
            "oCM2Inch_LengthInch_",
            PiCall < ("SelectSki", "cSelectSki_LengthInch_1", "cSelectSki_Brand_2", "cSelectSki_Model_3", "oSelectSki_lB_PriceUSD_Plus_Exception_rB_"),
            PiCut(
              "z2",
              "cCM2Inch_LengthCM_1",
              "oSelectLength_LengthCM_",
              PiCall < ("CM2Inch", "cCM2Inch_LengthCM_1", "oCM2Inch_LengthInch_"),
              PiCall < ("SelectLength", "cSelectLength_HeightCM_1", "cSelectLength_WeightKG_2", "oSelectLength_LengthCM_")
            )
          )
        ),
        PiCall < ("SelectModel", "cSelectModel_PriceLimit_1", "cSelectModel_SkillLevel_2", "oSelectModel_lB_Brand_x_Model_rB_")
      )
    )

    def apply(
        heightCM: HeightCM,
        priceLimit: PriceLimit,
        skillLevel: SkillLevel,
        weightKG: WeightKG
    )(implicit executor: ProcessExecutor[_]): Future[Either[PriceNOK, Exception]] = {
      implicit val context: ExecutionContext = executor.executionContext

      executor
        .execute(this, Seq(heightCM, priceLimit, skillLevel, weightKG))
        .map(_.asInstanceOf[Either[PriceNOK, Exception]])
    }
  }

}
