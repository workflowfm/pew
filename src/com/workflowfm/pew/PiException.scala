package com.workflowfm.pew

import com.workflowfm.pew.PiMetadata.SystemTime

sealed abstract class PiException[KeyT](message: String) extends Exception(message) {

  def id: KeyT
  def event: PiFailure[KeyT]
}

sealed trait HasPiInstance[KeyT] extends PiException[KeyT] {

  val pii: PiInstance[KeyT]
  override def id: KeyT = pii.id
}

case class NoResultException[KeyT](override val pii: PiInstance[KeyT])
    extends PiException[KeyT](s"[${pii.id}] NO RESULT!")
    with HasPiInstance[KeyT] {

  override def event: PiFailure[KeyT] = PiFailureNoResult(pii)
}

case class UnknownProcessException[KeyT](override val pii: PiInstance[KeyT], process: String)
    extends PiException[KeyT](s"[${pii.id}] FAILED - Unknown process: $process")
    with HasPiInstance[KeyT] {

  override def event: PiFailure[KeyT] = PiFailureUnknownProcess(pii, process)
}

case class AtomicProcessIsCompositeException[KeyT](
    override val pii: PiInstance[KeyT],
    process: String
) extends PiException[KeyT](
      s"[${pii.id}] FAILED - Executor encountered composite process thread: $process"
    )
    with HasPiInstance[KeyT] {

  override def event: PiFailure[KeyT] = PiFailureAtomicProcessIsComposite(pii, process)
}

case class NoSuchInstanceException[KeyT](override val id: KeyT)
    extends PiException[KeyT](s"[$id] FAILED - Failed to find instance!") {

  override def event: PiFailure[KeyT] = PiFailureNoSuchInstance(id)
}

class RemoteException[KeyT](
    override val id: KeyT,
    val message: String,
    val time: Long
) extends PiException[KeyT](message) {

  override def event: PiFailure[KeyT] =
    PiFailureExceptions(id, message, getStackTrace, PiMetadata(SystemTime -> time))
}

object RemoteException {

  def apply[KeyT](
      id: KeyT,
      message: String,
      trace: Array[StackTraceElement],
      time: Long
  ): RemoteException[KeyT] = {
    val exception = new RemoteException(id, message, time)
    exception.setStackTrace(trace)
    exception
  }

  def apply[KeyT](id: KeyT, t: Throwable): RemoteException[KeyT] =
    apply[KeyT](id, t.getMessage, t.getStackTrace, System.currentTimeMillis())
}

class RemoteProcessException[KeyT](
    override val id: KeyT,
    val ref: Int,
    val message: String,
    val time: Long
) extends PiException[KeyT](message) {

  override def event: PiFailure[KeyT] =
    PiFailureAtomicProcessException(id, ref, message, getStackTrace, PiMetadata(SystemTime -> time))
}

object RemoteProcessException {

  def apply[KeyT](
      id: KeyT,
      ref: Int,
      message: String,
      trace: Array[StackTraceElement],
      time: Long
  ): RemoteProcessException[KeyT] = {
    val exception = new RemoteProcessException(id, ref, message, time)
    exception.setStackTrace(trace)
    exception
  }

  def apply[KeyT](id: KeyT, ref: Int, t: Throwable): RemoteProcessException[KeyT] =
    apply[KeyT](id, ref, t.getMessage, t.getStackTrace, System.currentTimeMillis())
}
