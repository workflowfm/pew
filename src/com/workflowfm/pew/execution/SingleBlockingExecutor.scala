package com.workflowfm.pew.execution

import com.workflowfm.pew._
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.annotation.tailrec

/**
 * SingleBlockingExecutor fully executes one PiProcess from a map of given PiProcesses.
 * It blocks waiting for every atomic call to finish, so has no concurrency.
 */
case class SingleBlockingExecutor(processes:Map[String,PiProcess]) extends FutureExecutor {
  def call(name:String,args:PiObject*) = {
    processes get name match {
      case None => None
      case Some(process) => {
        val s = process.execState(args)
        System.err.println(" === INITIAL STATE === \n" + s + "\n === === === === === === === ===")
        val fs = run(s)
        System.err.println(" === FINAL STATE === \n" + fs + "\n === === === === === === === ===")
        val res = fs.resources.sub(process.output._1)
        if (res.isGround) Some(PiObject.get(res))
        else None
      }
    }
  }
  
  @tailrec
  final def run(s:PiState):PiState = {
    val ns = s.fullReduce()
    if (ns.threads isEmpty) ns
    else run((ns /: ns.threads)(handleThread))
  }
  
  def handleThread(s:PiState, x:(Int,PiFuture)):PiState = x match { case (ref,f) =>
    handleThread(ref,f,s) getOrElse s
  }
  
  def handleThread(ref:Int, f:PiFuture, s:PiState):Option[PiState] = f match {
    case PiFuture(name, outChan, args) => processes get name match {
      case None => {
        System.err.println("*** ERROR *** Unable to find process: " + name)
        Some(s removeThread ref)
      }
      case Some(p:AtomicProcess) => {
        val f = p.run(args map (_.obj))
        val res = Await.result(f,Duration.Inf)
        s.result(ref,res)
      }
      case Some(p:CompositeProcess) => { System.err.println("*** Executor encountered composite process thread: " + name); None } // TODO this should never happen!
    }
  }
  
  def withProc(p:PiProcess):SingleBlockingExecutor = copy(processes = processes + (p.name->p)) withProcs (p.dependencies :_*)
  def withProcs(l:PiProcess*):SingleBlockingExecutor = (this /: l)(_ withProc _)
  
  override def simulationReady:Boolean = true
  
  override def execute(process:PiProcess,args:Seq[Any]):Future[Future[Option[Any]]] =
    Future.successful(Future.successful(this withProc process call(process.name,args map PiObject.apply :_*)))
}
object SingleBlockingExecutor {
  def apply():SingleBlockingExecutor = SingleBlockingExecutor(Map[String,PiProcess]())
}
