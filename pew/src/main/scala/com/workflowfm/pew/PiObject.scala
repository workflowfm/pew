package com.workflowfm.pew

/**
  * A [[PiObject]] is a custom structure representing an object being passed around a pi-calculus workflow.
  * It mirrors the propositional CLL types, but is untyped at the item level.
  *
  * A [[PiObject]] can be:
  *   1. A pi-calculus '''channel''': [[Chan]].
  *   1. An '''item''': [[PiItem]].
  *   1. A '''pair''': [[PiPair]].
  *   1. An '''option''': [[PiOpt]].
  *   1. A '''Left''' or a '''Right''' selection: [[PiLeft]] and [[PiRight]]
  *
  */
sealed trait PiObject {

  /**
    * Checks if the [[PiObject]] contains free variables (channels).
    *
    * @return true if the object contains no free channels.
    */
  def isGround: Boolean = frees.isEmpty

  /**
    * Returns the list of free variables (channels) in the [[PiObject]].
    *
    * @return A sequence of free channels.
    */
  def frees: Seq[Chan] = Seq()

  /**
    * Updates the [[PiObject]] so that all free variables (channels) are fresh.
    *
    * @param i An integer to use to generate fresh names.
    * @return The updated [[PiObject]].
    */
  def fresh(i: Int): PiObject
}

object PiObject {

  /**
    * Converts any object into a [[PiObject]].
    * 
    * Pairs, `Left`, and `Right` are recursively converted to [[PiPair]], [[PiLeft]] and [[PiRight]] respectively. 
    * All other objects are wrapped in a [[PiItem]].
    *
    * @param a The object to convert.
    * @return The corresponding [[PiObject]].
    */
  def apply(a: Any): PiObject = a match {
    case (l, r) => PiPair(PiObject(l), PiObject(r))
    case Left(l) => PiLeft(PiObject(l))
    case Right(r) => PiRight(PiObject(r))
    case x => PiItem(x)
  }

  /**
    * Converts a [[PiObject]] to a Scala object.
    * 
    * @param o The [[PiObject]] object to convert.
    * @return The corresponding Scala object.
    */
  def get(o: PiObject): Any = o match {
    case Chan(s, i) => s + "#" + i
    case PiItem(i) => i
    case PiPair(l, r) => (get(l), get(r))
    case PiLeft(l) => Left(get(l))
    case PiRight(r) => Right(get(r))
    case PiOpt(l, r) => if (l.isGround) Left(get(l)) else Right(get(r))
  }

  /**
    * Converts a [[PiObject]] to a Scala object of a given type.
    * 
    * Same as [[get]] but also type casts the object. Type casting may fail, but we typically use this
    * when we can rely on typechecking from the reasoner with CLL.
    *
    * @tparam A The type of the resulting object.
    * @param o The [[PiObject]] to convert.
    * @return The corresponding Scala Object.
    */
  def getAs[A](o: PiObject): A = get(o).asInstanceOf[A]
}

/**
  * A pi-calculus channel.
  * 
  * These may be used for communication,
  * they may be communicated themselves, and they are also used as variables.
  *
  * For instance, channels are used as variables to create patterns of expected inputs and outputs.
  * For example, a process may expect a pair `PiPair(Chan(X),Chan(Y))` as input.
  * Each of the channels `X` and `Y` will (should) eventually be instantiated to a resource.
  * 
  * e.g. receiving input `a(X).0` from a matching output `'a<"Hello">.0` creates the instatiation
  * `X --> "Hello"`.
  * 
  * We apply these instantiations to the pattern `PiPair(Chan(X),Chan(Y))` until the
  * pattern contains no channels/variables. We then know that the input is available and
  * the process can be executed (pending all other inputs as well).
  * A process would never receive and/or do anything useful with a pi-calculus channel.
  *
  * @param s The name of the channel.
  * @param i An extra parameter to ensure freshness.
  */
case class Chan(s: String, i: Int = 0) extends PiObject {
  override val isGround: Boolean = false
  override lazy val frees: Seq[Chan] = Seq(this)
  override def fresh(f: Int): Chan = Chan(s, f)
}

object Chan {
  implicit def fromString(s: String): Chan = Chan(s, 0)
}

/**
  * A resource that can be communicated through pi-calculus channels.
  * 
  * This represents a resource (actually a container) that is treated as untyped in practice,
  * but its type corresponds to a CLL proposition. It is untyped so we can freely send it through
  * pi-calculus channels without the compiler complaining. Its correspondence with CLL ensures we
  * can type cast it without error when we need to obtain the actual resource.
  *
  * @param i The actual resource being carried.
  */
case class PiItem[+A](i: A) extends PiObject {
  override val isGround: Boolean = true
  override lazy val frees: Seq[Chan] = Seq()
  override def fresh(i: Int): PiItem[A] = this
}

/**
  * A pair of [[PiObject]]s.
  *
  * This corresponds to the `A (x) B` type in CLL.
  * 
  * @param l The first (left) element of the pair.
  * @param r The second (right) element of the pair.
  */
case class PiPair(l: PiObject, r: PiObject) extends PiObject {
  override val isGround: Boolean = l.isGround && r.isGround
  override lazy val frees: Seq[Chan] = l.frees ++ r.frees
  override def fresh(i: Int): PiPair = PiPair(l.fresh(i), r.fresh(i))
}

/**
  * A sum (option) between two [[PiObject]]s.
  * 
  * Only one of the two [[PiObject]]s will be used, similarly to the `Either` type 
  * and corresponding to the `A (+) B` type in CLL.
  *
  * @param l The first (left) option.
  * @param r The second (right) option.
  */
case class PiOpt(l: PiObject, r: PiObject) extends PiObject {
  override val isGround: Boolean = l.isGround || r.isGround
  override lazy val frees: Seq[Chan] = l.frees ++ r.frees
  override def fresh(i: Int): PiOpt = PiOpt(l.fresh(i), r.fresh(i))
}

/**
  * Representation of a left selection of an option.
  * 
  * This is used in [[LeftOut]] to build an output that selects the left option. 
  * In CLL, it corresponds to a `A (+) B` output type generated by the `(+)L` rule.
  *
  * @param l The object representing the selected type (`A`).
  */
case class PiLeft(l: PiObject) extends PiObject {
  override val isGround: Boolean = l.isGround
  override lazy val frees: Seq[Chan] = l.frees
  override def fresh(i: Int): PiLeft = PiLeft(l.fresh(i))
}

/**
  * Representation of a right selection of an option.
  * 
  * This is used in [[RightOut]] to build an output that selects the right option. 
  * In CLL, it corresponds to a `A (+) B` output type generated by the `(+)R` rule.
  *
  * @param r The object representing the selected type (`B`).
  */
case class PiRight(r: PiObject) extends PiObject {
  override val isGround: Boolean = r.isGround
  override lazy val frees: Seq[Chan] = r.frees
  override def fresh(i: Int): PiRight = PiRight(r.fresh(i))
}

/**
  * An instantion map of channels ([[Chan]]) to [[PiObject]].
  * 
  * They are used for substitution (especially in continuations) and to instantiate patterns.
  *
  * A [[ChanMap]] is a wrapper around a regular, immutable map.
  * 
  * @param map A mapping of [[Chan]]s to [[PiObject]]s.
  */
case class ChanMap(map: Map[Chan, PiObject] = Map()) {

  /**
    * Follows a chain of channel instantiations to produce the final channel.
    *
    * Sometimes chains of channel-to-channel mappings occur through pi-calculus communication.
    * 
    * e.g. X --> Y and Y --> Z
    * 
    * In that case, `resolve(Chan("X"))` returns the last channel `Z` in the chain. 
    * 
    * It ignores any non-channel PiObject that may come after that.
    * 
    * @param c The channel to be used as a starting point.
    * @return The last channel in the chain, or `c` if no chain is found.
    */
  def resolve(c: Chan): Chan =
    map.get(c) match {
      case None => c
      case Some(x: Chan) if c != x => resolve(x)
      case _ => c
    }

  /**
    * Returns the [[PiObject]] at the end of a chain of channel mappings.
    * 
    * Similar to [[com.workflowfm.pew.ChanMap.resolve(c* resolve]], but also returns
    * a [[PiObject]] instantiation at the end of the chain.
    * 
    * @see [[com.workflowfm.pew.ChanMap.resolve(c* resolve]]
    * @param c The channel to be used as a starting point.
    * @return The last object in the insantiation chain, or `c` if no chain is found.
    */
  def obtain(c: Chan): PiObject = {
    val cr = resolve(c)
    map.getOrElse(cr, cr)
  }

  /**
    * Applies the channel mapping as a substitution on a [[PiObject]].
    * 
    * Essentially recurses through the [[PiObject]] using [[obtain]] on any channel it finds.
    * 
    * @param o The [[PiObject]] to apply the substitution to.
    */
  def sub(o: PiObject): PiObject = {
    o match {
      case c: Chan => obtain(c)
      case i: PiItem[_] => i
      case PiPair(l, r) => PiPair(sub(l), sub(r))
      case PiOpt(l, r) => PiOpt(sub(l), sub(r))
      case PiLeft(l) => PiLeft(sub(l))
      case PiRight(r) => PiRight(sub(r))
    }
  }

  /**
    * Adds an insantiation to the mapping.
    *
    * @param c Channel to instantiate.
    * @param v Value to use in the instantiation.
    * @return The updated [[ChanMap]].
    */
  def +(c: Chan, v: PiObject): ChanMap = ChanMap(map + (c -> v))

  /**
    * Removes a channel instanation from the mapping.
    *
    * @param c The channel whose instantiation to remove.
    * @return The updated [[ChanMap]].
    */
  def -(c: Chan): ChanMap = ChanMap(map - c)

  /**
    * Removes the instantiations of a list of channels from the mapping.
    *
    * @param l The list of channels whose instantiations to remove.
    * @return The updated [[ChanMap]].
    */
  def --(l: Chan*): ChanMap = ChanMap(map -- l)

  /**
    * Append another [[ChanMap]].
    *
    * @note There is no composition of instantiations. 
    * Methods such as [[com.workflowfm.pew.ChanMap.resolve(c* resolve]] and [[obtain]] 
    * are used to follow chains of instantations instead.
    * 
    * @param m The [[ChanMap]] to append.
    * @return The updated [[ChanMap]].
    */
  def ++(m: ChanMap): ChanMap = ChanMap(map ++ m.map)

  /**
    * Add a map of channel instantations.
    *
    * @note There is no composition of instantiations. 
    * Methods such as [[com.workflowfm.pew.ChanMap.resolve(c* resolve]] and [[obtain]] 
    * are used to follow chains of instantations instead.
    *
    * @param m The map to add.
    * @return The updated [[ChanMap]].
    */
  def ++(m: Map[Chan, PiObject]): ChanMap = ChanMap(map ++ m)

  /**
    * Add a sequence of channel instantiations.
    *
    * @note There is no composition of instantiations. 
    * Methods such as [[com.workflowfm.pew.ChanMap.resolve(c* resolve]] and [[obtain]] 
    * are used to follow chains of instantations instead.
    *
    * @param l The sequence to add.
    * @return The updated [[ChanMap]].
    */
  def ++(l: Seq[(Chan, PiObject)]): ChanMap = ++(Map(l: _*))
}

object ChanMap {
  def apply(l: (Chan, PiObject)*): ChanMap = ChanMap(Map(l: _*))
}

/**
  * A pi-calculus resource (CLL type) annotated with a channel that carries it.
  * 
  * This effectively corresponds to an annotated CLL term in a sequent.
  *
  * @param obj The [[PiObject]] corresponding to the (type of that) resource.
  * @param c The channel that carries the resource.
  */
case class PiResource(obj: PiObject, c: Chan)

object PiResource {

  /**
    * A constructor of a [[PiResource]] that ensures the channel is instantiated as far as possible first.
    * 
    * Given a [[ChanMap]], this method runs [[com.workflowfm.pew.ChanMap.resolve(c* resolve]] first
    * to ensure the provided channel is as far instantiated as possible.
    *
    * @param obj The object of the [[PiResource]].
    * @param c The channel to be used as a starting point.
    * @param m The [[ChanMap]] used to resolve the channel.
    * @return The constructed [[PiResource]].
    */
  def of(obj: PiObject, c: Chan, m: ChanMap = ChanMap()): PiResource =
    PiResource(obj, m.resolve(c))
}
