package com.workflowfm.pew

sealed abstract class PiException[KeyT]
  extends Exception {

  def id: KeyT
}

sealed trait HasPiInstance[KeyT]
  extends PiException[KeyT] {

  val pii: PiInstance[KeyT]
  override def id: KeyT = pii.id
}

case class NoResultException[KeyT]( override val pii: PiInstance[KeyT] )
  extends PiException[KeyT] with HasPiInstance[KeyT]

case class UnknownProcessException[KeyT]( pii: PiInstance[KeyT], process: String )
  extends PiException[KeyT] with HasPiInstance[KeyT]

case class AtomicProcessIsCompositeException[KeyT]( pii: PiInstance[KeyT], process: String )
  extends PiException[KeyT] with HasPiInstance[KeyT]

case class NoSuchInstanceException[KeyT]( override val id: KeyT ) extends PiException[KeyT]

case class RemoteException[KeyT](
  override val id:  KeyT,
  message:          String,
  stackTrace:       String
) extends PiException[KeyT]

case class RemoteProcessException[KeyT](
  override val id: KeyT,
  ref: Int,
  message: String,
  stackTrace: String
) extends PiException[KeyT]

object PiException {
  implicit def apply[KeyT]: PiException[KeyT] => PiExceptionEvent[KeyT] = {
    case ex: NoResultException[KeyT]                  => PiFailureNoResult( ex.pii )
    case ex: UnknownProcessException[KeyT]            => PiFailureUnknownProcess( ex.pii, ex.process )
    case ex: AtomicProcessIsCompositeException[KeyT]  => PiFailureAtomicProcessIsComposite( ex.pii, ex.process )
    case ex: NoSuchInstanceException[KeyT]            => PiFailureNoSuchInstance( ex.id )
    case ex: RemoteException[KeyT]                    => PiEventException( ex.id, ex.message, ex.stackTrace, System.nanoTime() )
    case ex: RemoteProcessException[KeyT]             => PiEventProcessException( ex.id, ex.ref, ex.message, ex.stackTrace, System.nanoTime() )
  }
}
