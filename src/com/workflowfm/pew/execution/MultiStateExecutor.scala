package com.workflowfm.pew.execution

import com.workflowfm.pew._
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.annotation.tailrec
import scala.util.{Success,Failure}

/**
 * Executes any PiProcess asynchronously.
 * Only holds a single state, so can only execute one workflow at a time.
 * 
 * Running a second workflow after one has finished executing can be risky because 
 * promises/futures from the first workflow can trigger changes on the state!
 */

class MultiStateExecutor(var store:PiInstanceStore[Int], processes:PiProcessStore)(override implicit val context: ExecutionContext = ExecutionContext.global) extends FutureExecutor {
  def this(store:PiInstanceStore[Int], l:PiProcess*) = this(store,SimpleProcessStore(l :_*))
  def this(l:PiProcess*) = this(SimpleInstanceStore(),SimpleProcessStore(l :_*))
  
  var ctr:Int = 0
  val handle = new PromiseHandler[Int]
  
  def call(p:PiProcess,args:PiObject*) = store.synchronized {
	  val inst = PiInstance(ctr,p,args:_*)
	  val ret = handle.init(inst)
	  val ni = inst.reduce
    if (ni.completed) ni.result match {
		  case None => {
			  handle(PiEventFailure(ni,ProcessExecutor.NoResultException(ni.id.toString())))
		  }
		  case Some(res) => {
			  handle(PiEventResult(ni, res))
		  }
	  } else {
		  store = store.put(ni.handleThreads(handleThread(ni))._2)
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
      			  handle(PiEventFailure(ni,ProcessExecutor.NoResultException(ni.id.toString())))
      			  store = store.del(ni.id)
      		  }
      		  case Some(res) => {
      			  handle(PiEventResult(ni, res))
      			  store = store.del(ni.id)
      		  }
    		  } else {
    			  store = store.put(ni.handleThreads(handleThread(ni))._2)
    		  }
        }
	  }
  }
 
  def handleThread(i:PiInstance[Int])(ref:Int,f:PiFuture):Boolean = {
     System.err.println("*** [" + i.id + "] Handling thread: " + ref + " (" + f.fun + ")")
    f match {
    case PiFuture(name, outChan, args) => i.getProc(name) match {
      case None => {
        System.err.println("*** [" + i.id + "] ERROR *** Unable to find process: " + name)
        false
      }
      case Some(p:AtomicProcess) => {
        val objs = args map (_.obj)
        handle(PiEventCall(i.id,ref,p,objs))
        p.run(args map (_.obj)).onComplete{ 
          case Success(res) => {
            handle(PiEventReturn(i.id,ref,res))
            postResult(i.id,ref,res)
          }
          case Failure (ex) => handle(PiEventProcessException(i.id,ref,ex))
        }
        System.err.println("*** [" + i.id + "] Called process: " + p.name + " ref:" + ref)
        true
      }
      case Some(p:CompositeProcess) => { System.err.println("*** [" + i.id + "] Executor encountered composite process thread: " + name); false } // TODO this should never happen!
    }
  } }
    
  def postResult(id:Int,ref:Int, res:PiObject):Unit = {
    System.err.println("*** [" + id + "] Received result for thread " + ref + " : " + res)
    run(id,{x => x.postResult(ref, res)})
  }
 
  override def simulationReady:Boolean = store.simulationReady
  
  override def execute(process:PiProcess,args:Seq[Any]):Future[Future[Any]] =
    Future.successful(call(process,args map PiObject.apply :_*))
}
