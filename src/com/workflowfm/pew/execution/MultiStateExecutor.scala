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

class MultiStateExecutor(var store:PiInstanceStore[Int], processes:Map[String,PiProcess])(override implicit val context: ExecutionContext = ExecutionContext.global) extends FutureExecutor {
  def this(store:PiInstanceStore[Int], l:PiProcess*) = this(store,PiProcess.mapOf(l :_*))
  def this(l:PiProcess*) = this(SimpleInstanceStore(),PiProcess.mapOf(l :_*))
  
  var ctr:Int = 0
  val handler = new PromiseHandler[Int]
  
  def call(p:PiProcess,args:PiObject*) = store.synchronized {
	  val inst = PiInstance(ctr,p,args:_*)
	  val ret = handler.start(inst)
	  val ni = inst.reduce
    if (ni.completed) ni.result match {
		  case None => {
			  handler.failure(ni,ProcessExecutor.NoResultException(ni.id.toString()))
		  }
		  case Some(res) => {
			  handler.success(ni, res)
		  }
	  } else {
		  store = store.put(ni.handleThreads(handleThread(ni.id))._2)
	  }
	  ctr = ctr + 1
	  ret
  }
  
  final def run(id:Int,f:PiInstance[Int]=>PiInstance[Int]):Unit = store.synchronized {
	  store.get(id) match {
      case None => System.err.println("*** [" + id + "] No running instance! ***")
      case Some(i) => 
        if (i.id != id) System.err.println("*** [" + id + "] Different instance ID encountered: " + i.id)
        else {
          System.err.println("*** [" + id + "] Running!")
          val ni = f(i).reduce
    		  if (ni.completed) ni.result match {
      		  case None => {
      			  handler.failure(ni,ProcessExecutor.NoResultException(ni.id.toString()))
      			  store = store.del(ni.id)
      		  }
      		  case Some(res) => {
      			  handler.success(ni, res)
      			  store = store.del(ni.id)
      		  }
    		  } else {
    			  store = store.put(ni.handleThreads(handleThread(ni.id))._2)
    		  }
        }
	  }
  }
 
  def handleThread(id:Int)(ref:Int,f:PiFuture):Boolean = {
     System.err.println("*** [" + id + "] Handling thread: " + ref + " (" + f.fun + ")")
    f match {
    case PiFuture(name, outChan, args) => processes get name match {
      case None => {
        System.err.println("*** [" + id + "] ERROR *** Unable to find process: " + name)
        false
      }
      case Some(p:AtomicProcess) => {
        p.run(args map (_.obj)).onSuccess{ case res => postResult(id,ref,res) }
        System.err.println("*** [" + id + "] Called process: " + p.name + " ref:" + ref)
        true
      }
      case Some(p:CompositeProcess) => { System.err.println("*** [" + id + "] Executor encountered composite process thread: " + name); false } // TODO this should never happen!
    }
  } }
    
  def postResult(id:Int,ref:Int, res:PiObject):Unit = {
    System.err.println("*** [" + id + "] Received result for thread " + ref + " : " + res)
    run(id,{x => x.postResult(ref, res)})
  }
 
  override def execute(process:PiProcess,args:Seq[Any]):Future[Option[Any]] =
    call(process,args map PiObject.apply :_*)
}
