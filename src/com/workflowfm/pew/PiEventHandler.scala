package com.workflowfm.pew

import scala.concurrent.{Promise,Future}


sealed trait PiEvent[KeyT]

//case class PiEventStart[KeyT](i:PiInstance[KeyT]) extends PiEvent[KeyT]
case class PiEventResult[KeyT](i:PiInstance[KeyT], res:Any) extends PiEvent[KeyT]
case class PiEventFailure[KeyT](i:PiInstance[KeyT], reason:Throwable) extends PiEvent[KeyT]
case class PiEventException[KeyT](i:KeyT, reason:Throwable) extends PiEvent[KeyT]
case class PiEventCall[KeyT](id:KeyT, ref:Int, p:AtomicProcess, args:Seq[PiObject]) extends PiEvent[KeyT]
case class PiEventReturn[KeyT](id:KeyT, ref:Int, result:Any) extends PiEvent[KeyT]
case class PiEventProcessException[KeyT](id:KeyT, ref:Int, reason:Throwable) extends PiEvent[KeyT]


trait PiEventHandler[KeyT,InitT] extends (PiEvent[KeyT]=>Unit){
  def init(i:PiInstance[KeyT]):InitT
}

class DefaultHandler[T] extends PiEventHandler[T,Unit] {  
  override def init(i:PiInstance[T]):Unit = System.err.println(" === INITIAL STATE " + i.id + " === \n" + i.state + "\n === === === === === === === ===")
  
  override def apply(e:PiEvent[T]) = e match {
    case PiEventResult(i,res) => {
      System.err.println(" === FINAL STATE " + i.id + " === \n" + i.state + "\n === === === === === === === ===")
      System.err.println(" === RESULT FOR " + i.id + ": " + res)
    }
    case PiEventFailure(i,reason) => {	  
    	  System.err.println(" === FINAL STATE " + i.id + " === \n" + i.state + "\n === === === === === === === ===")
    	  System.err.println(" === FAILED: " + i.id + " ! === Exception: " + reason)
    	  reason.printStackTrace()
    }
    case PiEventException(id,reason) => {	  
  	    System.err.println(" === EXCEPTION: " + id + " ! === Exception: " + reason)
  	    reason.printStackTrace()
    }
    case PiEventCall(id,ref,p,args) => System.err.println(" === PROCESS CALL " + id + " === \n" + p.name + " (" + ref + ") with args: " + args.mkString(",") + "\n === === === === === === === ===")
    case PiEventReturn(id,ref,result) => System.err.println(" === PROCESS RETURN " + id + " === \n" + ref + " returned: " + result + "\n === === === === === === === ===")
    case PiEventProcessException(id,ref,reason) => {	  
    	  System.err.println(" === PROCESS FAILED: " + id + " === " + ref + " === Exception: " + reason)
    	  reason.printStackTrace()
     }
  }
}


class PromiseHandler[T] extends PiEventHandler[T,Future[Any]]{
  val default = new DefaultHandler[T]
  var promises:Map[T,Promise[Any]] = Map()

  override def apply(e:PiEvent[T]) = e match {
    case PiEventResult(i,res) => promises synchronized {
      default(e)
      promises.get(i.id) match {
        case None => Unit
        case Some(p) => p.success(res)
      }
      promises = promises - i.id
    }
    case PiEventFailure(i,reason) => promises synchronized {
      default(e)
      promises.get(i.id) match {
        case None => Unit
        case Some(p) => p.failure(reason)
      }
    }
    case PiEventException(id,reason) => promises synchronized {
      default(e)
      promises.get(id) match {
        case None => Unit
        case Some(p) => p.failure(reason)
      }
    }
    case PiEventProcessException(id,ref,reason) => promises synchronized {
      default(e)
      promises.get(id) match {
        case None => Unit
        case Some(p) => p.failure(reason)
      }
    }
    case _ => default(e)
  }  
  
  override def init(i:PiInstance[T]) = promises synchronized {
    default.init(i)
    val promise = Promise[Any]()
    promises += (i.id -> promise)
    promise.future
  }
  
}

object PromiseHandler {
  type ResultT = Future[Any]
}