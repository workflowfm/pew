package com.workflowfm.pew

import scala.concurrent.Future
import scala.concurrent.ExecutionContext

/**
 * A trait corresponding to any process that can be executed within our framework.
 * The specification generally follows the proofs-as-processes format, with PiObject patterns
 * representing CLL types, paired with strings representing the channel names. * 
 */
sealed trait PiProcess {
  def name:String // Name of the process and all its instances.
  def output:(PiObject,String) // Type (as a PiObject pattern) and channel of the output.
  def inputs:Seq[(PiObject,String)] // List of (type,channel) for each input.
  def channels:Seq[String] = output._2 +: (inputs map (_._2)) // order of channels is important for correct process calls!
      
  def dependencies:Seq[PiProcess] // dependencies of composite processes
  
  def allDependencies:Seq[PiProcess] = PiProcess.allDependenciesOf(this) // all ancestors (i.e. including dependencies of dependencies
  
  /**
   * Initializes a PiState that executes this process with a given list of PiObject arguments.
   * Generates an Output term from each PiObject in args so that they can be fed to the process.
   * Also generates an Input that consumes the output of the process when it is done.
   * Adds all dependencies to the state and then runs a PiCall directly.
   */
  def execState(args:Seq[PiObject]):PiState = {
		  val iTerms = args zip (inputs map (_._2)) map { case (o,c) => Output.of(o,c) }
		  val oTerm = Input.of(output._1,output._2)
			PiState() withProc this withProcs (allDependencies :_*) withTerms iTerms withTerm oTerm withTerm PiCall(name,channels map Chan) 
  } 
  
  /** 
   *  Used when a process is called to map argument channels to the given values.
   */
  def mapArgs(args:Chan*) = ChanMap(channels map Chan zip args:_*)

  def inputFrees() = inputs map (_._1.frees)
  
  override def toString = "[|" + name + "|]"
}
object PiProcess {
  def allDependenciesOf(p:PiProcess):Seq[PiProcess] =
    if (p.dependencies.isEmpty) Seq():Seq[PiProcess] 
    else p.dependencies ++ (p.dependencies flatMap allDependenciesOf)
  
  /**
   * Shortcut to create entries in name->PiProcess maps	
   */
	def entryOf(p:PiProcess):(String,PiProcess) = p.name->p
	/** 
	 * Shortcut to create name->PiProcess maps from a seq of processes.
	 */
	def mapOf(l:PiProcess*):Map[String,PiProcess] = (Map[String,PiProcess]() /: l)(_ + entryOf(_))
}


trait AtomicProcess extends PiProcess {
  /** 
   *  This function runs the process with the given PiObject arguments in an implicit execution context.
   *  Returns a future of the result as a PiObject.
   */
  def run(args:Seq[PiObject])(implicit ec:ExecutionContext):Future[PiObject]

  /**
   * This constructs a PiFuture to be added to the state when the process is called.
   */
  def getFuture(m:ChanMap):PiFuture = PiFuture(name,m.resolve(Chan(output._2)),inputs map { case (o,c) => PiResource.of(o,c,m) })
  def getFuture(args:Chan*):PiFuture = getFuture(mapArgs(args:_*))

  /** 
   *  This constructs the Input terms needed to appopriately receive the process inputs.
   */
  def getInputs(m:ChanMap):Seq[Input] = inputs map { case (o,c) => Input.of(m.sub(o),m.resolve(c)) }
  def getInputs(args:Chan*):Seq[Input] = getInputs(mapArgs(args:_*))
  
  override val dependencies:Seq[PiProcess] = Seq()
}

case class DummyProcess(override val name:String, override val channels:Seq[String], outChan:String, override val inputs:Seq[(PiObject,String)]) extends AtomicProcess {
  override val output = (PiItem(None),outChan)
  override def run(args:Seq[PiObject])(implicit ec:ExecutionContext):Future[PiObject] = Future.successful(output._1)
}


trait CompositeProcess extends PiProcess {
  /**
   * The body of the composition as a pi-calculus term constructed via proof. 
   */
  def body:Term
  
  /**
   * Calling function that instantiates the body with a given set of channel arguments.
   */
  def call(m:ChanMap):Term = body.sub(m)
  def call(args:Chan*):Term = call(mapArgs(args:_*))
}

case class DummyComposition(override val name:String, i:String, o:String, n:String) extends CompositeProcess {
  override val output = (Chan(n),o)
  override val inputs = Seq((Chan(n),i))
  override val channels = Seq(i,o)
  override val body = PiId(i,o,n)
  override val dependencies:Seq[PiProcess] = Seq()
}
