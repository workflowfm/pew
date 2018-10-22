package com.workflowfm.pew

sealed abstract class PiException[KeyT]( message: String )
  extends Exception( message ) {

  def id: KeyT

  def event: PiExceptionEvent[KeyT]
}

sealed trait HasPiInstance[KeyT]
  extends PiException[KeyT] {

  val pii: PiInstance[KeyT]
  override def id: KeyT = pii.id
}

case class NoResultException[KeyT]( override val pii: PiInstance[KeyT] )
  extends PiException[KeyT]( s"[${pii.id}] NO RESULT!" )
    with HasPiInstance[KeyT] {

  override def event: PiExceptionEvent[KeyT]
    = PiFailureNoResult( pii )
}

case class UnknownProcessException[KeyT]( override val pii: PiInstance[KeyT], process: String )
  extends PiException[KeyT]( s"[${pii.id}] FAILED - Unknown process: $process" )
    with HasPiInstance[KeyT] {

  override def event: PiExceptionEvent[KeyT]
    = PiFailureUnknownProcess( pii, process )
}

case class AtomicProcessIsCompositeException[KeyT]( override val pii: PiInstance[KeyT], process: String )
  extends PiException[KeyT]( s"[${pii.id}] FAILED - Executor encountered composite process thread: $process" )
    with HasPiInstance[KeyT] {

  override def event: PiExceptionEvent[KeyT]
    = PiFailureAtomicProcessIsComposite( pii, process )
}

case class NoSuchInstanceException[KeyT]( override val id: KeyT )
  extends PiException[KeyT]( s"[$id] FAILED - Failed to find instance!" ) {

  override def event: PiExceptionEvent[KeyT]
   = PiFailureNoSuchInstance( id )
}

case class RemoteException[KeyT](
  override val id:  KeyT,
  message:          String,
  stackTrace:       String
) extends PiException[KeyT]( message ) {

  override def event: PiExceptionEvent[KeyT]
    = PiEventException( id, message, stackTrace, System.nanoTime() )
}

case class RemoteProcessException[KeyT](
  override val id: KeyT,
  ref: Int,
  message: String,
  stackTrace: String
) extends PiException[KeyT]( message ) {

  override def event: PiExceptionEvent[KeyT]
    = PiEventProcessException( id, ref, message, stackTrace, System.nanoTime() )
}
