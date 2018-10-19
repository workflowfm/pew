package com.workflowfm.pew.execution

import com.workflowfm.pew._

import scala.concurrent._
import scala.util.{Failure, Success}

/**
 * Executes any PiProcess asynchronously.
 * Only holds a single state, so can only execute one workflow at a time.
 * 
 * Running a second workflow after one has finished executing can be risky because 
 * promises/futures from the first workflow can trigger changes on the state!
 */

class MultiStateExecutor(var store:PiInstanceStore[Int], processes:PiProcessStore)(override implicit val context: ExecutionContext = ExecutionContext.global) extends SimulatorExecutor[Int] with SimplePiObservable[Int] {
  def this(store:PiInstanceStore[Int], l:PiProcess*) = this(store,SimpleProcessStore(l :_*))
  def this(l:PiProcess*) = this(SimpleInstanceStore[Int](),SimpleProcessStore(l :_*))
  
  var ctr:Int = 0
  
  override protected def init(p:PiProcess,args:Seq[PiObject]) = store.synchronized {
	  val inst = PiInstance(ctr,p,args:_*)
	  store = store.put(inst)
	  ctr = ctr + 1
	  Future.successful(ctr-1)
  }

  override def start(id:Int):Unit = store.get(id) match {
    case None => publish(PiFailureNoSuchInstance(id))
    case Some(inst) => {
      publish(PiEventStart(inst))
  	  val ni = inst.reduce
      if (ni.completed) ni.result match {
  		  case None => {
  			  publish(PiFailureNoResult(ni))
  			  store = store.del(id)
  		  }
  		  case Some(res) => {
  			  publish(PiEventResult(ni, res))
  			  store = store.del(id)
  		  }
  	  } else {
  		  val (_,resi) = ni.handleThreads(handleThread(ni))
  			store = store.put(resi)
  	  }
    }
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
      			  publish(PiFailureNoResult(ni))
      			  store = store.del(ni.id)
      		  }
      		  case Some(res) => {
      			  publish(PiEventResult(ni, res))
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
        publish(PiEventCall(i.id,ref,p,objs))
        p.run(args map (_.obj)).onComplete{ 
          case Success(res) => {
            publish(PiEventReturn(i.id,ref,res))
            postResult(i.id,ref,res)
          }
          case Failure (ex) => publish(PiEventProcessException(i.id,ref,ex))
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
}
