package com.workflowfm.pew

/**
  * A Term is a pi-calculus pattern that can be encountered in a proofs-as-processes composition.
  */
sealed trait Term {
  def sub(s: ChanMap): Term
  def fresh(i: Int): Term
  def addTo(s: PiState): PiState
}

/**
  * A ChannelTerm is a Term that depends on a particular channel to operate.
  * Essentially the equivalent of a pi-calculus continuation.
  */
sealed trait ChannelTerm extends Term {
  def channel: Chan
}

/**
  * Input Terms for proofs-as-processes compositions.
  */
sealed trait Input extends ChannelTerm {
  /**
    * Returns True if the Input is able to receive a given PiObject.
    * For example, a PiLeft can only be received by an optional input.
    */
  def admits(a: PiObject): Boolean

  /**
    * This passes a PiObject to be received through this Input's channel.
    * Returns the list of Terms (continuations) that follow and a (potential)
    * channel mapping of any received resources.
    */
  def receive(a: PiObject): (List[Term], ChanMap)

  override def sub(s: ChanMap): ChannelTerm
  override def addTo(s: PiState): PiState = s.copy(inputs = s.inputs + (this.channel -> this))
}

object Input {

  /**
    * Creates an Input that can receive the pattern described by the PiObject through the given channel.
    */
  def of(res: PiObject, channel: String): Input = res match {
    case PiPair(l, r) =>
      ParIn(channel, channel + "#L", channel + "#R", of(l, channel + "#L"), of(r, channel + "#R")) // TODO must be unique channels! Can we get these from the prover?
    case PiOpt(l, r) =>
      WithIn(channel, channel + "#L", channel + "#R", of(l, channel + "#L"), of(r, channel + "#R"))
    case Chan(s) => Devour(channel, s)
    case _ => Devour(channel, channel + "##")
  }
}

/**
  * c(v).0
  * Receives any PiObject through channel c and maps it to channel v.
  */
case class Devour(c: String, v: String) extends Input {
  override val channel: Chan = Chan(c)
  override def admits(a: PiObject): Boolean = true
  override def sub(s: ChanMap): Devour = Devour(s.resolve(c), v)
  override def fresh(i: Int): Devour = Devour(c + "#" + i, v)

  override def receive(a: PiObject): (List[Term], ChanMap) = (List(), ChanMap((Chan(v), a)))
}

/**
  * c(v).cont
  * Receives an atomic PiObject (channel or item) through channel c, maps it to channel v,
  * applies the resulting substitution to the continuation and returns the result.
  */
case class In(c: String, v: String, cont: Term) extends Input {
  override val channel: Chan = Chan(c)

  override def admits(a: PiObject): Boolean = a match {
    case Chan(_) => true
    case PiItem(_) => true
    case _ => false
  }
  override def sub(s: ChanMap): In = In(s.resolve(c), s.resolve(v), cont.sub(s)) // TODO this may need improvement to enforce name binding.
  override def fresh(i: Int): In = In(c + "#" + i, v, cont.fresh(i))

  override def receive(a: PiObject): (List[Term], ChanMap) =
    (List(cont.sub(ChanMap((Chan(v), a)))), ChanMap())
}

/**
  * c(lv,rv).(left | right)
  * Parallel input. Receives a PiPair, instantiates each continuation appropriately and returns them.
  */
case class ParIn(c: String, lv: String, rv: String, left: Term, right: Term) extends Input {
  override val channel: Chan = Chan(c)

  override def admits(a: PiObject): Boolean = a match {
    case PiPair(_, _) => true
    case _ => false
  }

  override def sub(s: ChanMap): ParIn =
    ParIn(s.resolve(c), s.resolve(lv), s.resolve(rv), left.sub(s), right.sub(s)) // TODO this may need improvement to enforce name binding.
  override def fresh(i: Int): ParIn =
    ParIn(c + "#" + i, lv + "#" + i, rv + "#" + i, left.fresh(i), right.fresh(i))

  override def receive(a: PiObject): (List[Term], ChanMap) = a match {
    case PiPair(l, r) =>
      (List(left.sub(ChanMap((Chan(lv), l))), right.sub(ChanMap((Chan(rv), r)))), ChanMap())
    case _ => (List(this), ChanMap())
  }
}

/**
  * c(lv,rv).cont
  * Parallel input with single continuation. This occurs from the application of the Par CLL rule.
  */
case class ParInI(c: String, lv: String, rv: String, cont: Term) extends Input {
  override val channel: Chan = Chan(c)

  override def admits(a: PiObject): Boolean = a match {
    case PiPair(_, _) => true
    case _ => false
  }
  override def sub(s: ChanMap): ParInI = ParInI(s.resolve(c), s.resolve(lv), s.resolve(rv), cont.sub(s)) // TODO this may need improvement to enforce name binding.
  override def fresh(i: Int): ParInI = ParInI(c + "#" + i, lv + "#" + i, rv + "#" + i, cont.fresh(i))

  override def receive(a: PiObject): (List[Term], ChanMap) = a match {
    case PiPair(l, r) => (List(cont.sub(ChanMap((Chan(lv), l), (Chan(rv), r)))), ChanMap())
    case _ => (List(this), ChanMap())
  }
}

/**
  * c(lv,rv).(left + right)
  * Optional input. Can only receive a PiLeft or PiRight. Instantiates and returns the corresponding continuation.
  */
case class WithIn(c: String, lv: String, rv: String, left: Term, right: Term) extends Input {
  override val channel: Chan = Chan(c)

  override def admits(a: PiObject): Boolean = a match {
    case PiLeft(_) => true
    case PiRight(_) => true
    case _ => false
  }

  override def sub(s: ChanMap): WithIn =
    WithIn(s.resolve(c), s.resolve(lv), s.resolve(rv), left.sub(s), right.sub(s)) // TODO this may need improvement to enforce name binding.
  override def fresh(i: Int): WithIn =
    WithIn(c + "#" + i, lv + "#" + i, rv + "#" + i, left.fresh(i), right.fresh(i))

  override def receive(a: PiObject): (List[Term], ChanMap) = a match {
    case PiLeft(l) => (List(left.sub(ChanMap((Chan(lv), l)))), ChanMap())
    case PiRight(r) => (List(right.sub(ChanMap((Chan(rv), r)))), ChanMap())
    case _ => (List(this), ChanMap())
  }
}

/**
  * Output Terms for proofs-as-processes compositions.
  */
sealed trait Output extends ChannelTerm {
  /**
    * Sends its output through the given PiState
    * Returns a list of continuations, the PiObject it wants to send, and an updated PiState.
    * We often need to update the freshness counter of the state, hence the 3rd output.
    */
  def send(s: PiState): (List[Term], PiObject, PiState)
  override def addTo(s: PiState): PiState = s.copy(outputs = s.outputs + (this.channel -> this))
}

object Output {

  /**
    * Creates an Output that can send the given PiObject through the given channel.
    */
  def of(res: PiObject, channel: String): Output = res match {
    case PiPair(l, r) =>
      ParOut(channel, channel + "#L", channel + "#R", of(l, channel + "#L"), of(r, channel + "#R")) // TODO must be unique channels! Can we get these from the prover?
    case PiLeft(l) => LeftOut(channel, channel + "#L", of(l, channel + "#L"))
    case PiRight(r) => RightOut(channel, channel + "#R", of(r, channel + "#R"))
    case _ => Out(channel, res)
  }
}

/**
  * 'c<a>.0
  * Sends a PiObject through a channel.
  */
case class Out(c: String, a: PiObject) extends Output {
  override val channel: Chan = Chan(c)
  override def sub(s: ChanMap): Out = Out(s.resolve(c), s.sub(a))
  override def fresh(i: Int): Out = Out(c + "#" + i, a)
  override def send(s: PiState): (List[Term], PiObject, PiState) = (List(), a, s)
}

/**
  * 'c<lc,rc>.(left | right)
  * Parallel output.
  * In proofs-as-processes, parallel communication (i.e. of a pair of objects) happens in 2 stages:
  * 1) Use a common channel to send 2 new channels, one for the left side of the pair and one for the right.
  * 2) Communicate each part of the pair through its respective channel.
  * This performs the output for the first stage.
  * Creates fresh versions of lc and rc and then sends them out as a pair.
  */
case class ParOut(c: String, lc: String, rc: String, left: Term, right: Term) extends Output {
  override val channel: Chan = Chan(c)

  override def sub(s: ChanMap): ParOut =
    ParOut(s.resolve(c), s.resolve(lc), s.resolve(rc), left.sub(s), right.sub(s))

  override def fresh(i: Int): ParOut =
    ParOut(c + "#" + i, lc + "#" + i, rc + "#" + i, left.fresh(i), right.fresh(i))

  override def send(s: PiState): (List[Term], PiObject, PiState) = {
    val freshlc = lc + "#" + s.freshCtr
    val freshrc = rc + "#" + s.freshCtr
    val m = ChanMap((Chan(lc), Chan(freshlc)), (Chan(rc), Chan(freshrc)))
    (List(left.sub(m), right.sub(m)), PiPair(Chan(freshlc), Chan(freshrc)), s.incFCtr())
  }
}

/**
  * 'c<lc>.cont
  * Optional left output.
  * We do not have sums here. Optional communication happens through pairs of channels as the parallel one.
  * However, WithIn only anticipates a PiLeft or a PiRight.
  * This returns a PiLeft with a fresh channel to receive the actual resource from.
  *
  * Note that this is different from the proofs-as-processes way of handling optional communication.
  * In that, channel polarity gets flipped. i.e. the output channel *receives* the 2 channels for each of
  * the options. We avoid this here.
  */
case class LeftOut(c: String, lc: String, cont: Term) extends Output {
  override val channel: Chan = Chan(c)
  override def sub(s: ChanMap): LeftOut = LeftOut(s.resolve(c), s.resolve(lc), cont.sub(s))
  override def fresh(i: Int): LeftOut = LeftOut(c + "#" + i, lc + "#" + i, cont.fresh(i))

  override def send(s: PiState): (List[Term], PiObject, PiState) = {
    val freshlc = lc + "#" + s.freshCtr
    val m = ChanMap((Chan(lc), Chan(freshlc)))
    (List(cont.sub(m)), PiLeft(Chan(freshlc)), s.incFCtr())
  }
}

/**
  * 'c<rc>.cont
  * Optional right output.
  * We do not have sums here. Optional communication happens through pairs of channels as the parallel one.
  * However, WithIn only anticipates a PiLeft or a PiRight.
  * This returns a PiRight with a fresh channel to receive the actual resource from.
  *
  * Note that this is different from the proofs-as-processes way of handling optional communication.
  * In that, channel polarity gets flipped. i.e. the output channel *receives* the 2 channels for each of
  * the options. We avoid this here.
  */
case class RightOut(c: String, rc: String, cont: Term) extends Output {
  override val channel: Chan = Chan(c)
  override def sub(s: ChanMap): RightOut = RightOut(s.resolve(c), s.resolve(rc), cont.sub(s))
  override def fresh(i: Int): RightOut = RightOut(c + "#" + i, rc + "#" + i, cont.fresh(i))

  override def send(s: PiState): (List[Term], PiObject, PiState) = {
    val freshrc = rc + "#" + s.freshCtr
    val m = ChanMap((Chan(rc), Chan(freshrc)))
    (List(cont.sub(m)), PiRight(Chan(freshrc)), s.incFCtr())
  }
}

/**
  * i(n).'o<n>.0
  * Axiom buffer
  */
object PiId {
  def apply(i: String, o: String, n: String): Input = In(i, n, Out(o, Chan(n)))
}

/**
  * Proofs-as-processes cut pattern.
  * Substitutes channels lc in left and rc in right with a fresh version of z, then adds them to the state.
  * In a proofs-as-processes generated composition, one (currently the left) will be an input and the other
  * will be an output, so this will force them to communicate with each other.
  * It is nice, however, that polymorphism on withTerm allows us to not specify which is which, so we are closer to
  * the pure pi-calculus.
  */
case class PiCut(z: String, lc: String, rc: String, left: Term, right: Term) extends Term {

  override def sub(s: ChanMap): PiCut = {
    val sl = s - Chan(lc)
    val sr = s - Chan(rc)
    PiCut(z, lc, rc, left.sub(sl), right.sub(sr))
  }

  override def fresh(i: Int): PiCut =
    PiCut(z + "#" + i, lc + "#" + i, rc + "#" + i, left.fresh(i), right.fresh(i))

  override def addTo(s: PiState): PiState = {
    val freshz = z + "#" + s.freshCtr
    s incFCtr () withTerm left.sub(ChanMap((Chan(lc), Chan(freshz)))) withTerm right.sub(
      ChanMap((Chan(rc), Chan(freshz)))
    )
  }
}

/**
  * Represents a pending call to an atomic process in 2 situations:
  * 1) A call has been encountered in the workflow, but we are waiting for the inputs to arrive.
  * 2) The inputs have arrived, we have made the call to the actual process, and we are waiting for it to return.
  * @param fun the name of the process
  * @param outChan the channel through which we should send the output when it arrives
  * @param args the process inputs, each including a pattern of the input and the corresponding channel it will arrive through
  */
case class PiFuture(fun: String, outChan: Chan, args: Seq[PiResource]) {
  /**
    *  We call this once the process output has arrived, to create the appropriate Output pi-calculus term.
    */
  def toOutput(res: PiObject): Output = Output.of(res, outChan.s)

  def sub(s: ChanMap): PiFuture = copy(args = args map {
        case PiResource(o, c) => PiResource(s.sub(o), c)
      })

  /**
    * We call this to check if all inputs have arrived and we can execute the process.
    * @return an instantiated instance where all inputs are ground so we can make the call
    *      to the actual process, or None if not all inputs have arrived
    */
  def execute(m: ChanMap): Option[PiFuture] = {
    val newFut = sub(m)
    if (newFut.args exists (!_.obj.isGround)) None
    else Some(newFut)
  }
}

/**
  * A call to an atomic process in the composition.
  * @param name the name of the process being called
  * @param args the (channel) parameters given
  */
case class PiCall(name: String, args: Seq[Chan]) extends Term {
  override def sub(s: ChanMap): PiCall = copy(args = args map s.resolve)
  override def fresh(i: Int): PiCall = PiCall(name, args map (_.fresh(i)))

  override def addTo(s: PiState): PiState = s.handleCall(this)
}

object PiCall {
  def <(n: String, args: String*): PiCall = PiCall(n, args map Chan)
}
