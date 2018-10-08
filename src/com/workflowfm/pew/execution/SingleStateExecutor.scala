package com.workflowfm.pew.execution

import com.workflowfm.pew._
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.annotation.tailrec
import scala.util.{Success, Failure}

/**
 * Executes any PiProcess asynchronously.
 * Only holds a single state, so can only execute one workflow at a time.
 * 
 * Running a second workflow after one has finished executing can be risky because 
 * promises/futures from the first workflow can trigger changes on the state!
 */

class SingleStateExecutor(processes:PiProcessStore)(override implicit val context: ExecutionContext = ExecutionContext.global) extends ProcessExecutor[Int] with SimplePiObservable[Int] {
  def this(l:PiProcess*) = this(SimpleProcessStore(l :_*))
  
  var ctr:Int = 0
  var instance:Option[PiInstance[Int]] = None
  
  override def run(p:PiProcess,args:Seq[PiObject]):Future[Int] = {
    if (instance.isDefined) Future.failed(new ProcessExecutor.AlreadyExecutingException())
    else {
      val inst = PiInstance(ctr,p,args:_*)
      System.err.println(" === INITIAL STATE === \n" + inst.state + "\n === === === === === === === ===")
      instance = Some(inst)
      ctr = ctr + 1
      run
      Future.successful(ctr)
    }
  }

  def success(id:Int,res:Any) = {
    instance match {
      case Some(i) => {
        System.err.println(" === FINAL STATE === \n" + i.state + "\n === === === === === === === ===")
        publish(PiEventResult(i,res))
      }
      case None => publish(PiEventException(id,new ProcessExecutor.NoSuchInstanceException(id.toString)))
    }
    instance = None
  }
  
  def failure(inst:PiInstance[Int]) = {
	  System.err.println(" === FINAL STATE === \n" + inst.state + "\n === === === === === === === ===")
	  publish(PiEventFailure(inst,new ProcessExecutor.NoResultException(inst.id.toString)))
	  instance = None 
  }
  
  final def run:Unit = this.synchronized {
    instance match {
      case None => Unit
      case Some(i) =>
        val ni = i.reduce
        if (ni.completed) {
          ni.result match {
            case None => failure(ni)
            case Some(res) => success(ni.id,res)        
        }} else {
          instance = Some(ni.handleThreads(handleThread(ni.id))._2)
        }
    }}
 
  def handleThread(id:Int)(ref:Int,f:PiFuture):Boolean = f match {
    case PiFuture(name, outChan, args) => processes get name match {
      case None => {
        publish(PiEventException(id,new ProcessExecutor.UnknownProcessException(name)))
        false
      }
      case Some(p:AtomicProcess) => {
        val objs = args map (_.obj)
        p.run(objs).onComplete{ 
          case Success(res) => {
            postResult(id,ref,res)
            publish(PiEventReturn(id,ref,res))
          }
          case Failure(ex) => {
            publish(PiEventProcessException(id,ref,ex))
          }
        }
        publish(PiEventCall(id,ref,p,objs))
        true
      }
      case Some(p:CompositeProcess) => { System.err.println("*** Executor encountered composite process thread: " + name); false } // TODO this should never happen!
    }
  }
    
  def postResult(id:Int,ref:Int, res:PiObject):Unit = this.synchronized {
    System.err.println("*** Received result for ID:" + id + " Thread:" + ref + " : " + res)
    instance match {
      case None => publish(PiEventException(id,new ProcessExecutor.NoSuchInstanceException(id.toString)))
      case Some(i) => 
        if (i.id != id) publish(PiEventException(id,new ProcessExecutor.NoSuchInstanceException(id.toString)))
        else {
          instance = Some(i.postResult(ref, res))
          run
        }
    }
  }
 
  def simulationReady:Boolean = instance match {
    case None => true
    case Some(i) => i.simulationReady
  }
  
}