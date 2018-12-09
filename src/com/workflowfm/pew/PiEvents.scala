package com.workflowfm.pew

import com.workflowfm.pew._

sealed trait PiEvent[KeyT] {
  def id:KeyT
  def asString:String
  val time:Long
}

/** PiEvents which are associated with a specific AtomicProcess call.
  */
sealed trait PiAtomicProcessEvent[KeyT] extends PiEvent[KeyT] {
  def ref: Int
}

sealed trait PiExceptionEvent[KeyT] extends PiEvent[KeyT] {
  def exception: PiException[KeyT]

  /** Jev, Override `toString` method as printing entire the entire trace by default gets old.
    */
  override def toString: String = {
    val ex: PiException[KeyT] = exception

    val typeName: String = ex.getClass.getSimpleName
    val message: String = ex.getMessage

    s"PiEe($typeName):$id($message)"
  }
}

case class PiEventStart[KeyT](i:PiInstance[KeyT], override val time:Long=System.currentTimeMillis()) extends PiEvent[KeyT] {
  override def id = i.id
  override def asString = " === [" + i.id + "] INITIAL STATE === \n" + i.state + "\n === === === === === === === ==="
}


case class PiEventResult[KeyT](i:PiInstance[KeyT], res:Any, override val time:Long=System.currentTimeMillis()) extends PiEvent[KeyT] {
  override def id = i.id
  override def asString = " === [" + i.id + "] FINAL STATE === \n" + i.state + "\n === === === === === === === ===\n" +
      " === [" + i.id + "] RESULT: " + res
}

case class PiEventCall[KeyT](override val id:KeyT, ref:Int, p:AtomicProcess, args:Seq[PiObject], override val time:Long=System.currentTimeMillis())
  extends PiAtomicProcessEvent[KeyT] {

	override def asString: String = s" === [$id] PROCESS CALL: ${p.name} ($ref) args: ${args.mkString(",")}"
}

case class PiEventReturn[KeyT](override val id:KeyT, ref:Int, result:Any, override val time:Long=System.currentTimeMillis())
  extends PiAtomicProcessEvent[KeyT] {

	override def asString: String = s" === [$id] PROCESS RETURN: ($ref) returned: $result"
}

case class PiFailureNoResult[KeyT](i:PiInstance[KeyT], override val time:Long=System.currentTimeMillis()) extends PiEvent[KeyT] with PiExceptionEvent[KeyT] {
  override def id: KeyT = i.id
  override def asString: String = s" === [$id] FINAL STATE ===\n${i.state}\n === === === === === === === ===\n === [$id] NO RESULT! ==="

  override def exception: PiException[KeyT] = NoResultException[KeyT]( i )
}

case class PiFailureUnknownProcess[KeyT](i:PiInstance[KeyT], process:String, override val time:Long=System.currentTimeMillis()) extends PiExceptionEvent[KeyT] {
  override def id: KeyT = i.id
	override def asString: String = s" === [$id] FINAL STATE === \n${i.state}\n === === === === === === === ===\n" +
			s" === [$id] FAILED - Unknown process: $process"

  override def exception: PiException[KeyT] = UnknownProcessException[KeyT]( i, process )
}

case class PiFailureAtomicProcessIsComposite[KeyT]( i: PiInstance[KeyT], process: String , override val time:Long=System.currentTimeMillis()) extends PiExceptionEvent[KeyT] {
  override def id: KeyT = i.id
	override def asString: String = s" === [$id] FINAL STATE === \n${i.state}\n === === === === === === === ===\n" +
			s" === [$id] FAILED - Executor encountered composite process thread: $process"

  override def exception: PiException[KeyT] = AtomicProcessIsCompositeException[KeyT]( i, process )
}

case class PiFailureNoSuchInstance[KeyT](override val id:KeyT, override val time:Long=System.currentTimeMillis()) extends PiExceptionEvent[KeyT] {
	override def asString: String = s" === [$id] FAILED - Failed to find instance!"

  override def exception: PiException[KeyT] = NoSuchInstanceException[KeyT]( id )
}

case class PiEventException[KeyT](override val id:KeyT, message:String, trace: Array[StackTraceElement], override val time:Long) extends PiExceptionEvent[KeyT] {
	override def asString: String = s" === [$id] FAILED - Exception: $message\n === [$id] Trace: $trace"

  override def exception: PiException[KeyT] = RemoteException[KeyT]( id, message, trace, time )

  override def equals( other: Any ): Boolean = other match {
    case that: PiEventException[KeyT] =>
      id == that.id && message == that.message
  }
}

case class PiEventProcessException[KeyT](override val id:KeyT, ref:Int, message:String, trace: Array[StackTraceElement], override val time:Long)
  extends PiEvent[KeyT] with PiAtomicProcessEvent[KeyT] with PiExceptionEvent[KeyT] {

	override def asString: String = s" === [$id] PROCESS [$ref] FAILED - Exception: $message\n === [$id] Trace: $trace"

  override def exception: PiException[KeyT] = RemoteProcessException[KeyT]( id, ref, message, trace, time )

  override def equals( other: Any ): Boolean = other match {
    case that: PiEventProcessException[KeyT] =>
      id == that.id && ref == that.ref && message == that.message
  }
}

object PiEventException {
  def apply[KeyT]( id: KeyT, ex: Throwable, time: Long=System.currentTimeMillis() ): PiEventException[KeyT]
    = PiEventException( id, ex.getLocalizedMessage, ex.getStackTrace, time )
}

object PiEventProcessException {
  def apply[KeyT]( id: KeyT, ref: Int, ex: Throwable, time: Long=System.currentTimeMillis() ): PiEventProcessException[KeyT]
    = PiEventProcessException( id, ref, ex.getLocalizedMessage, ex.getStackTrace, time )
}
