package com.workflowfm.pew

import scala.concurrent.{Promise,Future}

trait PiEventHandler[KeyT,InitT] {
  def start(i:PiInstance[KeyT]):InitT
  def success(i:PiInstance[KeyT], res:Any):Unit = Unit
  def failure(inst:PiInstance[KeyT],reason:Option[Throwable]=None):Unit  = Unit
}

class DefaultHandler[T] extends PiEventHandler[T,Unit] {  
  override def start(i:PiInstance[T]):Unit = {
    System.err.println(" === INITIAL STATE " + i.id + " === \n" + i.state + "\n === === === === === === === ===")
  }
  
  override def success(i:PiInstance[T], res:Any) = {
    System.err.println(" === FINAL STATE " + i.id + " === \n" + i.state + "\n === === === === === === === ===")
    System.err.println(" === RESULT FOR " + i.id + ": " + res)
  }
  
  override def failure(i:PiInstance[T],reason:Option[Throwable]=None) = {	  
	  System.err.println(" === FINAL STATE " + i.id + " === \n" + i.state + "\n === === === === === === === ===")
	  System.err.println(" === FAILED: " + i.id + " ! === Exception: " + reason)
  }
}

class PromiseHandler[T] extends PiEventHandler[T,Future[Option[Any]]]{
  val default = new DefaultHandler[T]
  var promises:Map[T,Promise[Option[Any]]] = Map()
 
  override def start(i:PiInstance[T]) = promises synchronized {
    default.start(i)
    val promise = Promise[Option[Any]]()
    promises += (i.id -> promise)
    promise.future
  }
  
  override def success(i:PiInstance[T], res:Any) = promises synchronized {
    default.success(i,res)
    promises.get(i.id) match {
      case None => Unit
      case Some(p) => p.success(Some(res))
    }
    promises = promises - i.id
  }
  
  override def failure(i:PiInstance[T],reason:Option[Throwable]=None) = promises synchronized {
    default.failure(i,reason)
    promises.get(i.id) match {
      case None => Unit
      case Some(p) => reason match {
        case None => p.success(None)
        case Some(ex) => p.failure(ex)
      }
    }
  }
}