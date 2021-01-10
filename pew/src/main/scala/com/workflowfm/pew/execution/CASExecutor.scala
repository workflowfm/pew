package com.workflowfm.pew.execution

import java.util.concurrent.ConcurrentHashMap
import java.util.UUID

import scala.concurrent._
import scala.util.{ Failure, Success }

import com.workflowfm.pew._
import com.workflowfm.pew.stream.SimplePiObservable

/**
  * A multi-state Compare-And-Swap [[ProcessExecutor]].
  * 
  * Uses a [[java.util.concurrent.ConcurrentHashMap ConcurrentHashMap]] to store states
  * in a thread-safe way. 
  * 
  * Updates states using compare-and-swap for a finite number of attempts. 
  * This is to avoid the situation of multiple asynchronous atomic processes posting their results
  * on the same state at the same time. 
  *
  * @param maxCASAttempts The maximum number of Compare-And-Swap attempts before we fail.
  * @param executionContext
  */
class CASExecutor (final val maxCASAttempts: Int = 10)(
    implicit override val executionContext: ExecutionContext
) extends ProcessExecutor[UUID]
    with SimplePiObservable[UUID] {

  val store = new ConcurrentHashMap[UUID, PiInstance[UUID]]

  override protected def init(instance: PiInstance[_]): Future[UUID] = {
    val uuid = UUID.randomUUID
    store.put(uuid, instance.copy(id = uuid))
    Future.successful(uuid)
  }

  override def start(id: UUID): Unit = run(id, { x => x })

  private def run(id: UUID, f: PiInstance[UUID] => PiInstance[UUID]): Unit = {
    if (!store.containsKey(id)) {
      publish(PiFailureNoSuchInstance(id))
    } else {
      var cas: Option[Int] = Some(0)
      while (cas.getOrElse(maxCASAttempts) < maxCASAttempts) { // Keep trying until we get Some(maxCASAttempts) or None
        val i = store.get(id)
        if (i.id != id) {
          System.err.println("*** [" + id + "] Different instance ID encountered: " + i.id)
          cas = None
        } else {
          val ni = f(i).reduce
          if (ni.completed) ni.result match {
            case None => {
              publish(PiFailureNoResult(ni))
              store.remove(ni.id)
              cas = None
            }
            case Some(res) => {
              publish(PiEventResult(ni, res))
              store.remove(ni.id)
              cas = None
            }
          }
          else {
            val (toCall, resi) = ni.handleThreads(handleThread(ni))
            if (store.replace(id, i, resi)) {
              toCall.foreach(runThread(resi))
              cas = None
            } else {
              cas = cas.map(_ + 1)
            }
          }
        }
      }
      if (cas.isDefined) publish(PiFailureExceptions(id, CASExecutor.CASFailureException(id, maxCASAttempts)))
    }
  }

  private def handleThread(i: PiInstance[UUID])(ref: Int, f: PiFuture): Boolean = {
    f match {
      case PiFuture(name, _, _) =>
        i.getProc(name) match {
          case None => {
            publish(PiFailureUnknownProcess(i, name))
            false
          }
          case Some(_: MetadataAtomicProcess) => true
          case Some(_: CompositeProcess) => {
            publish(PiFailureAtomicProcessIsComposite(i, name))
            false
          } // TODO this should never happen!
        }
    }
  }

  private def runThread(i: PiInstance[UUID])(ref: Int): Unit = {
    i.piFutureOf(ref) match {
      case None => Unit
      case Some(PiFuture(name, _, args)) =>
        i.getProc(name) match {
          case None => {
            publish(PiFailureUnknownProcess(i, name))
          }
          case Some(p: MetadataAtomicProcess) => {
            val objs = args map (_.obj)
            publish(PiEventCall(i.id, ref, p, objs))
            p.runMeta(args map (_.obj)).onComplete {
              case Success(res) => {
                publish(PiEventReturn(i.id, ref, PiObject.get(res._1), res._2))
                postResult(i.id, ref, res._1)
              }
              case Failure(ex) => publish(PiFailureAtomicProcessException(i.id, ref, ex))
            }
          } 
          case Some(_: CompositeProcess) => {
            publish(PiFailureAtomicProcessIsComposite(i, name))
          } // TODO this should never happen!
        }
    }
  }

  def postResult(id: UUID, ref: Int, res: PiObject): Unit = {
    run(id, { x => x.postResult(ref, res) })
  }
}

object CASExecutor {
  final case class CASFailureException(id: UUID, max: Int, private val cause: Throwable = None.orNull)
      extends Exception(
        "Compare-and-swap failed after " + max + " attempts for id: " + id,
        cause
      )
}
