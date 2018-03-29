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

class MultiStateExecutor(var store:PiInstanceStore[Int],processes:Map[String,PiProcess])(override implicit val context: ExecutionContext = ExecutionContext.global) extends ProcessExecutor {
  def this(store:PiInstanceStore[Int],l:PiProcess*) = this(store,PiProcess.mapOf(l :_*))
  def this(l:PiProcess*) = this(SimpleInstanceStore(),PiProcess.mapOf(l :_*))
  
  var ctr:Int = 0
  var promises:Map[Int,Promise[Option[Any]]] = Map()
  
  def call(p:PiProcess,args:PiObject*) = store.synchronized {
	  val inst = PiInstance(ctr,p,args:_*)
	  val promise = Promise[Option[Any]]()
	  System.err.println(" === INITIAL STATE " + ctr + " === \n" + inst.state + "\n === === === === === === === ===")
	  promises += (ctr -> promise)
	  ctr = ctr + 1
	  run(inst)
	  promise.future
  }

  def update(i:PiInstance[Int]) = store.synchronized {
    store = store.put(i)
  }
  
  def clean(id:Int) = store.synchronized {
    store = store.del(id)
    promises = promises - id
  }
  
  def success(i:PiInstance[Int], res:Any) = {
    System.err.println(" === FINAL STATE " + i.id + " === \n" + i.state + "\n === === === === === === === ===")
    promises.get(i.id) match {
      case Some(p) => p.success(Some(res))
      case None => Unit
    }
    clean(i.id)
  }
  
  def failure(inst:PiInstance[Int]) = {
	  System.err.println(" === Failed to obtain output " + inst.id + " ! ===")
	  System.err.println(" === FINAL STATE " + inst.id + " === \n" + inst.state + "\n === === === === === === === ===")
    promises.get(inst.id) match {
      case Some(p) => p.success(None)
      case None => Unit
    }
	  clean(inst.id)
  }
  
  final def run(i:PiInstance[Int]):Unit = store.synchronized {
	  val ni = i.reduce
	  if (ni.completed) ni.result match {
		  case None => failure(ni)
		  case Some(res) => success(i,res)        
		} else {
			update(ni.handleThreads(handleThread(ni.id)))
		}
  }
 
  def handleThread(id:Int)(ref:Int,f:PiFuture):Boolean = f match {
    case PiFuture(name, outChan, args) => processes get name match {
      case None => {
        System.err.println("*** ERROR *** Unable to find process: " + name)
        false
      }
      case Some(p:AtomicProcess) => {
        p.run(args map (_.obj)).onSuccess{ case res => postResult(id,ref,res)}
        System.err.println("*** Called process: " + p.name + " id:" + id + " ref:" + ref)
        true
      }
      case Some(p:CompositeProcess) => { System.err.println("*** Executor encountered composite process thread: " + name); false } // TODO this should never happen!
    }
  }
    
  def postResult(id:Int,ref:Int, res:PiObject):Unit = store.synchronized {
    System.err.println("*** Received result for ID:" + id + " Thread:" + ref + " : " + res)
    store.get(id) match {
      case None => System.err.println("*** No running instance! ***")
      case Some(i) => 
        if (i.id != id) System.err.println("*** Different instance ID encountered: " + i.id)
        else {
          run(i.postResult(ref, res))
        }
    }
  }
 
  override def execute(process:PiProcess,args:Seq[Any]):Future[Option[Any]] =
    call(process,args map PiObject.apply :_*)
}