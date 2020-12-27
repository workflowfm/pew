package com.workflowfm.pew

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import com.workflowfm.pew.MetadataAtomicProcess.MetadataAtomicResult
import com.workflowfm.pew.PiMetadata.{ PiMetadataElem, PiMetadataMap }

/**
  * A trait corresponding to any process that can be executed within our framework.
  * The specification generally follows the proofs-as-processes format, with PiObject patterns
  * representing CLL types, paired with strings representing the channel names. *
  */
sealed trait PiProcess {
  def name: String // Name of the process
  def iname: String = name // This can be used to identify different instances of this process that run
  // on different workflows in the same executor.
  // Note that each workflow can only support one instance of a process.
  def output: (PiObject, String) // Type (as a PiObject pattern) and channel of the output.
  def inputs: Seq[(PiObject, String)] // List of (type,channel) for each input.
  lazy val channels: Seq[Chan] = Chan(output._2, 0) +: (inputs map { case (_,i) => Chan(i, 0) }) // order of channels is important for correct process calls!

  val dependencies: Seq[PiProcess] // dependencies of composite processes

  lazy val allDependencies: Seq[PiProcess] = PiProcess.allDependenciesOf(this) // all ancestors (i.e. including dependencies of dependencies

  /**
    * Initializes a PiState that executes this process with a given list of PiObject arguments.
    * Generates an Output term from each PiObject in args so that they can be fed to the process.
    * Also generates an Input that consumes the output of the process when it is done.
    * Adds all dependencies to the state and then runs a PiCall directly.
    */
  def execState(args: Seq[PiObject]): PiState = {
    val iTerms = args zip (inputs map (_._2)) map { case (o, c) => Output.of(o, Chan(c, 0)) }
    val oTerm = Input.of(output._1, Chan(output._2, 0))
    PiState() withProc this withProcs (allDependencies: _*) withTerms iTerms withTerm oTerm withTerm PiCall(
      name,
      channels
    )
  }

  /**
    *  Used when a process is called to map argument channels to the given values.
    */
  def mapArgs(args: Chan*): ChanMap = ChanMap(channels zip args: _*)

  def mapFreshArgs(i: Int, args: Chan*): ChanMap = ChanMap(
    channels map (_.fresh(i)) zip args: _*
  )

  def inputFrees(): Seq[Seq[Chan]] = inputs map (_._1.frees)

  override def toString: String = "[|" + name + "|]"

  /**
    * Shortcut to create entries in name->PiProcess maps
    */
  def toEntry: (String, PiProcess) = name -> this

  /**
    * * Shortcut to create entries in name->PiProcess maps using the instance name
    */
  def toIEntry: (String, PiProcess) = iname -> this
}

object PiProcess {

  def allDependenciesOf(p: PiProcess): Seq[PiProcess] =
    if (p.dependencies.isEmpty) Seq(): Seq[PiProcess]
    else p.dependencies ++ (p.dependencies flatMap allDependenciesOf)
}

trait MetadataAtomicProcess extends PiProcess {
  /**
    *  This function runs the process with the given PiObject arguments in an implicit execution context.
    * Allows the child processes to return arbitrary meta-data
    * which can be incorporated into the PiEvent history using supported ProcessExecutors.
    *
    * @return A future containing the result of the result as a PiObject and optional meta-data.
    */
  def runMeta(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[MetadataAtomicResult]

  /**
    * This constructs a PiFuture to be added to the state when the process is called.
    */
  def getFuture(i: Int, m: ChanMap): PiFuture = PiFuture(
    name,
    m.resolve(Chan(output._2, 0).fresh(i)),
    inputs map { case (o, c) => PiResource.of(o.fresh(i), Chan(c, 0).fresh(i), m) }
  )
  def getFuture(i: Int, args: Chan*): PiFuture = getFuture(i, mapFreshArgs(i, args: _*))

  /**
    *  This constructs the Input terms needed to appopriately receive the process inputs.
    */
  def getInputs(i: Int, m: ChanMap): Seq[Input] = inputs map {
      case (o, c) => Input.of(m.sub(o.fresh(i)), m.resolve(Chan(c, 0).fresh(i)))
    }
  def getInputs(i: Int, args: Chan*): Seq[Input] = getInputs(i, mapFreshArgs(i, args: _*))

  override val dependencies: Seq[PiProcess] = Seq()

}

trait AtomicProcess extends MetadataAtomicProcess {

  /** Implements the standard AtomicProcess interface for unsupporting ProcessExecutors.
    */
  override def runMeta(args: Seq[PiObject])(
      implicit ec: ExecutionContext
  ): Future[MetadataAtomicResult] = run(args).map(MetadataAtomicProcess.result(_))

  def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject]

}

object MetadataAtomicProcess {

  type MetadataAtomicResult = (PiObject, PiMetadataMap)

  /** Syntactic sugar for returning results from MetadataAtomicProcesses.
    *
    * @param result Result of the AtomicProcess.
    * @param data Metadata elements with their keys.
    * @return Tuple of Result and Meta-data map.
    */
  def result(result: PiObject, data: PiMetadataElem[_]*): MetadataAtomicResult =
    (result, PiMetadata(data: _*))

  /** Upgrade AtomicProcess to MetadataAtomicProcesses so ProcessExecutors
    * can simply integrate existing process.
    *
    * @return A MetadataAtomicProcess wrapper around the input AtomicProcess
    *         *IFF* it's not already a MetadataAtomicProcess.
    */
  /* implicit def from: AtomicProcess => MetadataAtomicProcess = { case existing:
   * MetadataAtomicProcess => existing
   *
   * case original: AtomicProcess => new MetadataAtomicProcess { override def name: String =
   * original.name override def output: (PiObject, String) = original.output override def inputs:
   * Seq[(PiObject, String)] = original.inputs
   *
   * override def runMeta(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[(PiObject,
   * PiMetadataMap)]
   * = original.run(args).map((_, PiMetadata())) } } */
}

case class DummyProcess(
    override val name: String,
    outChan: String,
    override val inputs: Seq[(PiObject, String)]
) extends AtomicProcess {
  override val output: (PiItem[None.type], String) = (PiItem(None), outChan)

  override def run(args: Seq[PiObject])(implicit ec: ExecutionContext): Future[PiObject] =
    Future.successful(output._1)
}

trait CompositeProcess extends PiProcess {
  /**
    * The body of the composition as a pi-calculus term constructed via proof.
    */
  def body: Term

  /**
    * Calling function that instantiates the body with a given set of channel arguments.
    */
  def call(m: ChanMap): Term = body.sub(m)
  def call(args: Chan*): Term = call(mapArgs(args: _*))
}

case class DummyComposition(override val name: String, i: String, o: String, n: String)
    extends CompositeProcess {
  override val output: (Chan, String) = (Chan(n, 0), o)
  override val inputs: Seq[(Chan, String)] = Seq((Chan(n, 0), i))
  override val body: Input = PiId(Chan(i, 0), Chan(o, 0), Chan(n, 0))
  override val dependencies: Seq[PiProcess] = Seq()
}

trait PiProcessStore {
  /**
    * Get a PiProcess by its *instance* name.
    */
  def get(name: String): Option[PiProcess]
  def getAll: Seq[PiProcess]

  def getOrElse[B >: PiProcess](name: String, default: => B): B = get(name) match {
    case None => default
    case Some(r) => r
  }
  def entryOf(name: String): Option[(String, PiProcess)] = get(name) map (_.toEntry)
  def toMap: Map[String, PiProcess] = Map(getAll map (_.toEntry): _*)
  def toIMap: Map[String, PiProcess] = Map(getAll map (_.toIEntry): _*)
}

object PiProcessStore {
  /**
    * Shortcut to create name->PiProcess maps from a seq of processes.
    * These are used within PiState.
    */
  def mapOf(l: PiProcess*): Map[String, PiProcess] = (Map[String, PiProcess]() /: l)(_ + _.toEntry)
}

case class SimpleProcessStore(m: Map[String, PiProcess]) extends PiProcessStore {
  override def get(name: String): Option[PiProcess] = m.get(name)
  override def getAll: Seq[PiProcess] = m.values.toSeq
}

object SimpleProcessStore {

  def apply(l: PiProcess*): SimpleProcessStore = SimpleProcessStore(
    (Map[String, PiProcess]() /: l)(_ + _.toIEntry)
  )
  /* def procs(l:PiProcess*):SimpleProcessStore = SimpleProcessStore((Map[String,PiProcess]() /: l)
   * (_ + _.toEntry)) */
}
