package com.workflowfm.pew.stream

import com.workflowfm.pew.{ PiEvent, PiExceptionEvent, PiEventResult }

import java.text.SimpleDateFormat

import scala.collection.immutable.Queue

// Return true if the handler is done and needs to be unsubscribed.

trait PiEventHandler[KeyT] extends (PiEvent[KeyT]=>Boolean) {
  def name:String
  def and(h:PiEventHandler[KeyT]) = MultiPiEventHandler(this,h)
}

trait PiEventHandlerFactory[T,H <: PiEventHandler[T]] {
  def build(id:T):H
}

class PrintEventHandler[T](override val name:String) extends PiEventHandler[T] {   
  val formatter = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss.SSS")
  override def apply(e:PiEvent[T]) = {
    val time = formatter.format(e.time)
    System.err.println(s"[$time] ${e.asString}")
    false
  }
}


case class MultiPiEventHandler[T](handlers:Queue[PiEventHandler[T]]) extends PiEventHandler[T] {
  override def name = handlers map (_.name) mkString(",")
  override def apply(e:PiEvent[T]) = handlers map (_(e)) forall (_ == true)
  override def and(h:PiEventHandler[T]) = MultiPiEventHandler(handlers :+ h)
}
  
object MultiPiEventHandler {
  def apply[T](handlers:PiEventHandler[T]*):MultiPiEventHandler[T] = MultiPiEventHandler[T](Queue[PiEventHandler[T]]() ++ handlers)
}


