package com.workflowfm.pew

import scala.concurrent.Future
import scala.concurrent.ExecutionContext

/**
 * A trait corresponding to any process that can be executed within our framework.
 * The specification generally follows the proofs-as-processes format, with PiObject patterns
 * representing CLL types, paired with strings representing the channel names. * 
 */
sealed trait PiProcess {
  def name:String // Name of the process 
  def iname:String = name // This can be used to identify different instances of this process that run
                          // on different workflows in the same executor.
                          // Note that each workflow can only support one instance of a process.
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
  def mapFreshArgs(i:Int,args:Chan*) = ChanMap(channels map (_ + "#" + i) map Chan zip args:_*)

  def inputFrees() = inputs map (_._1.frees)
  
  override def toString = "[|" + name + "|]"
  
  /**
   * Shortcut to create entries in name->PiProcess maps	
   */
	def toEntry:(String,PiProcess) = name->this 
	
	/**
   * * Shortcut to create entries in name->PiProcess maps using the instance name	
   */
	def toIEntry:(String,PiProcess) = iname->this 
}
object PiProcess {
  def allDependenciesOf(p:PiProcess):Seq[PiProcess] =
    if (p.dependencies.isEmpty) Seq():Seq[PiProcess] 
    else p.dependencies ++ (p.dependencies flatMap allDependenciesOf)
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
  def getFuture(i:Int,m:ChanMap):PiFuture = PiFuture(name,m.resolve(Chan(output._2).fresh(i)),inputs map { case (o,c) => PiResource.of(o.fresh(i),c + "#" + i,m) })
  def getFuture(i:Int,args:Chan*):PiFuture = getFuture(i,mapFreshArgs(i,args:_*))

  /** 
   *  This constructs the Input terms needed to appopriately receive the process inputs.
   */
  def getInputs(i:Int,m:ChanMap):Seq[Input] = inputs map { case (o,c) => Input.of(m.sub(o.fresh(i)),m.resolve(c+"#"+i)) }
  def getInputs(i:Int,args:Chan*):Seq[Input] = getInputs(i,mapFreshArgs(i,args:_*))
  
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


trait PiProcessStore {
  /**
   * Get a PiProcess by its *instance* name.
   */
  def get(name:String):Option[PiProcess]
  def getAll:Seq[PiProcess]
  
  def getOrElse[B >: PiProcess](name:String,default: => B):B = get(name) match {
    case None => default
    case Some(r) => r
  }
  def entryOf(name:String):Option[(String,PiProcess)] = get(name) map (_.toEntry)
  def toMap:Map[String,PiProcess] = Map(getAll map (_.toEntry) :_*)
  def toIMap:Map[String,PiProcess] = Map(getAll map (_.toIEntry) :_*)
}
object PiProcessStore {
  /** 
	 * Shortcut to create name->PiProcess maps from a seq of processes.
	 * These are used within PiState.
	 */
	def mapOf(l:PiProcess*):Map[String,PiProcess] = (Map[String,PiProcess]() /: l)(_ + _.toEntry)
}

case class SimpleProcessStore(m:Map[String,PiProcess]) extends PiProcessStore {
  override def get(name:String):Option[PiProcess] = m.get(name)
  override def getAll:Seq[PiProcess] = m.values.toSeq
}
object SimpleProcessStore {
  def apply(l:PiProcess*):SimpleProcessStore = SimpleProcessStore((Map[String,PiProcess]() /: l) (_ + _.toIEntry))
  //def procs(l:PiProcess*):SimpleProcessStore = SimpleProcessStore((Map[String,PiProcess]() /: l) (_ + _.toEntry))
}
