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
      instance = Some(inst)
      ctr = ctr + 1
      run
      Future.successful(ctr - 1)
    }
  }

  def success(id:Int,res:Any) = {
    instance match {
      case Some(i) => {
        publish(PiEventResult(i,res))
      }
      case None => publish(PiEventException(id,new ProcessExecutor.NoSuchInstanceException(id.toString)))
    }
    instance = None
  }
  
  def failure(inst:PiInstance[Int]) = {
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
          instance = Some(ni.handleThreads(handleThread(ni))._2)
        }
    }}
 
  def handleThread(i:PiInstance[Int])(ref:Int,f:PiFuture):Boolean = f match {
    case PiFuture(name, outChan, args) => i.getProc(name) match {
      case None => {
        publish(PiEventFailure(i,new ProcessExecutor.UnknownProcessException(name)))
        false
      }
      case Some(p:AtomicProcess) => {
        val objs = args map (_.obj)
        publish(PiEventCall(i.id,ref,p,objs))
        p.run(objs).onComplete{ 
          case Success(res) => {
            publish(PiEventReturn(i.id,ref,res))
            postResult(i.id,ref,res)
          }
          case Failure(ex) => {
            publish(PiEventProcessException(i.id,ref,ex))
            instance = None 
          }
        }
        true
      }
      case Some(p:CompositeProcess) => { System.err.println("*** Executor encountered composite process thread: " + name); false } // TODO this should never happen!
    }
  }
    
  def postResult(id:Int,ref:Int, res:PiObject):Unit = this.synchronized {
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