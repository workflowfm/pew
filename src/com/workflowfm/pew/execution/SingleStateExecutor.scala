package com.workflowfm.pew.execution

import com.workflowfm.pew._
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.annotation.tailrec

/**
 * Executes any PiProcess asynchronously.
 * Only holds a single state, so can only execute one workflow at a time.
 * 
 * Running a second workflow after one has finished executing can be risky because 
 * promises/futures from the first workflow can trigger changes on the state!
 */

class SingleStateExecutor(processes:Map[String,PiProcess])(override implicit val context: ExecutionContext = ExecutionContext.global) extends ProcessExecutor {
  def this(l:PiProcess*) = this(PiProcess.mapOf(l :_*))
  
  var ctr:Int = 0
  var instance:Option[PiInstance[Int]] = None
  var promise:Promise[Option[Any]] = Promise()
  
  def call(p:PiProcess,args:PiObject*) = {
    if (instance.isDefined) Future.failed(new ProcessExecutor.AlreadyExecutingException())
    else {
      val inst = PiInstance(ctr,p,args:_*)
      System.err.println(" === INITIAL STATE === \n" + inst.state + "\n === === === === === === === ===")
      instance = Some(inst)
      ctr = ctr + 1
      run
      promise.future
    }
  }

  def success(res:Any) = {
    instance match {
      case Some(i) => System.err.println(" === FINAL STATE === \n" + i.state + "\n === === === === === === === ===")
      case None => System.err.println(" === FINAL STATE IS GONE!! === === === === === === === === ===")
    }
    promise.success(Some(res)) 
    instance = None
    promise = Promise()  
  }
  
  def failure(inst:PiInstance[Int]) = {
	  System.err.println(" === Failed to obtain output! ===")
	  System.err.println(" === FINAL STATE === \n" + inst.state + "\n === === === === === === === ===")
	  promise.success(None) 
	  instance = None
	  promise = Promise()  
  }
  
  final def run:Unit = this.synchronized {
    instance match {
      case None => Unit
      case Some(i) =>
        val ni = i.reduce
        if (ni.completed) {
          ni.result match {
            case None => failure(ni)
            case Some(res) => success(res)        
        }} else {
          instance = Some(ni.handleThreads(handleThread(ni.id)))
        }
    }}
 
  def handleThread(id:Int)(ref:Int,f:PiFuture):Boolean = f match {
    case PiFuture(name, outChan, args) => processes get name match {
      case None => {
        System.err.println("*** ERROR *** Unable to find process: " + name)
        false
      }
      case Some(p:AtomicProcess) => {
        p.run(args map (_.obj)).onSuccess{ case res => postResult(id,ref,res)}
        System.err.println("*** Called process: " + p.name + " ref:" + ref)
        true
      }
      case Some(p:CompositeProcess) => { System.err.println("*** Executor encountered composite process thread: " + name); false } // TODO this should never happen!
    }
  }
    
  def postResult(id:Int,ref:Int, res:PiObject):Unit = this.synchronized {
    System.err.println("*** Received result for ID:" + id + " Thread:" + ref + " : " + res)
    instance match {
      case None => System.err.println("*** No running instance! ***")
      case Some(i) => 
        if (i.id != id) System.err.println("*** Different instance ID encountered: " + i.id)
        else {
          instance = Some(i.postResult(ref, res))
          run
        }
    }
  }
 
  override def execute(process:PiProcess,args:Seq[Any]):Future[Option[Any]] =
    call(process,args map PiObject.apply :_*)
}