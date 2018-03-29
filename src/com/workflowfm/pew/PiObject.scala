package com.workflowfm.pew

/**
 * A PiObject is a custom structure representing an object being passed around a pi-calculus workflow.
 * It mirrors the propositional CLL types, but is untyped at the item level.
 * 
 * A PiObject can be:
 * 1) A pi-calculus channel: These are used in multiple ways. They may be used for communication,
 * they may be communicated themselves, and they are also used as variables (see below).
 * 2) An item: This represents a resource (actually a container) that is untyped, in practice, 
 * but its type corresponds to a CLL proposition. It is untyped so we can freely send it through 
 * pi-calculus channels without the compiler complaining. Its correspondence with CLL ensures we 
 * can type cast it without error when we need to obtain the actual resource.
 * 3) A pair of PiObjects corresponding to the Pair Scala type and the CLL tensor/times operator.
 * 4) An option between 2 PiObjects corresponding to the Either Scala type and the CLL plus operator.
 * 5) A Left or a Right PiObject corresponding to the instances of the Either Scala type (used as
 * runtime optional outputs).
 * 
 * Channels as variables:
 * We use Chan PiObjects as variables to create patterns of expected inputs and outputs.
 * For example, a process may expect a pair PiPair(Chan(X),Chan(Y)) as input. 
 * Each of the channels X and Y will (should) eventually be instantiated to a resource,
 * e.g. receiving input a(X).0 from a matching output 'a<"Hello">.0 creates the instatiation
 * X --> "Hello"
 * We apply these instantiations to the pattern PiPair(Chan(X),Chan(Y)) until the 
 * pattern contains no channels/variables. We then know that the input is available and
 * the process can be executed (pending all other inputs as well).
 * A process would never receive and/or do anything useful with a pi-calculus channel.
 * 
 */
sealed trait PiObject {
  def isGround:Boolean = frees.isEmpty
  def frees:Seq[Chan] = Seq()
}
object PiObject {
  def apply(a:Any):PiObject = a match {
    case (l,r) => PiPair(PiObject(l),PiObject(r))
    case Left(l) => PiLeft(PiObject(l))
    case Right(r) => PiRight(PiObject(r))
    case x => PiItem(x)
  }
  
  def get(o:PiObject):Any = o match {
    case Chan(s) => s
    case PiItem(i) => i
    case PiPair(l,r) => (get(l),get(r))
    case PiLeft(l) => Left(get(l))
    case PiRight(r) => Right(get(r))
    case PiOpt(l,r) => if (l.isGround) Left(get(l)) else Right(get(r)) 
  }
  
  def getAs[A](o:PiObject) = get(o).asInstanceOf[A]
}

case class Chan(s:String) extends PiObject {
  override def isGround:Boolean = false
  override def frees:Seq[Chan] = Seq(this)
}
case class PiItem[+A](i:A) extends PiObject {
  override def isGround:Boolean = true
  override val frees:Seq[Chan] = Seq()
}
case class PiPair(l:PiObject,r:PiObject) extends PiObject {
  override def isGround:Boolean = l.isGround && r.isGround
  override def frees:Seq[Chan] = l.frees ++ r.frees
}

case class PiOpt(l:PiObject,r:PiObject) extends PiObject {
  override def isGround:Boolean = l.isGround || r.isGround
  override def frees:Seq[Chan] = l.frees ++ r.frees
}
case class PiLeft(l:PiObject) extends PiObject {
  override def isGround:Boolean = l.isGround
  override def frees:Seq[Chan] = l.frees
}
case class PiRight(r:PiObject) extends PiObject {
  override def isGround:Boolean = r.isGround
  override def frees:Seq[Chan] = r.frees
}


/**
 * ChanMaps represent instantiations of pi-calculus channels (Chan) to other PiObjects.
 * They are used for substitution (especially in continuations) and to instantiate patterns.
 * 
 */
case class ChanMap(map:Map[Chan,PiObject] = Map()) {
  
  /**
   * Sometimes chains of channel-to-channel mappings occur through pi-calculus communication.
   * e.g. X --> Y and Y --> Z
   * This returns the last channel (Z) in such a chain. It ignores any non-channel PiObject that may
   * come after that.
   */
  def resolve(c:Chan):Chan =
			map.get(c) match {
			case None => c
			case Some(Chan(x)) if c.s != x => resolve(Chan(x))
			case _ => c
	}
  
  def resolve(s:String):String = resolve(Chan(s)).s
  
  /**
   * Returns the PiObject at the end of a chain of channel mappings. 
   */
	def obtain(c:Chan):PiObject = {
    val cr = resolve(c)
    map.getOrElse(cr,cr)
  }

  /**
   * Applies the channel mapping as a substitution on a PiObject.
   */
	def sub(o:PiObject):PiObject = {
    o match {
      case Chan(c) => obtain(Chan(c))
      case PiItem(i) => PiItem(i)
      case PiPair(l,r) => PiPair(sub(l),sub(r))
      case PiOpt(l,r) => PiOpt(sub(l),sub(r))
      case PiLeft(l) => PiLeft(sub(l))
      case PiRight(r) => PiRight(sub(r))
    }
  }

  def +(c:Chan,v:PiObject):ChanMap = ChanMap(map + (c->v))
  def -(c:Chan):ChanMap = ChanMap(map - c)
  def --(l:Chan*):ChanMap = ChanMap(map -- l)
  def ++(m:ChanMap):ChanMap = ChanMap(map ++ m.map)
  def ++(m:Map[Chan,PiObject]):ChanMap = ChanMap(map ++ m)
  def ++(l:Seq[(Chan,PiObject)]):ChanMap = ++(Map(l:_*))
}

object ChanMap {
  def apply(l:(Chan,PiObject)*):ChanMap = ChanMap(Map(l:_*))
}


case class PiResource(obj:PiObject,c:Chan)
object PiResource {
  def of(obj:PiObject,c:String,m:ChanMap=ChanMap()):PiResource = PiResource(obj,m.resolve(Chan(c)))
}