package com.workflowfm.pew.execution

import scala.concurrent._
import scala.util.{ Failure, Success }

import com.workflowfm.pew._
import com.workflowfm.pew.stream.SimplePiObservable

/**
  * A multi-state mutex-based [[ProcessExecutor]].
  *
  * Uses `this.synchronized` to create mutexes for safe access when adding/removing/updating states.
  *
  * @param store An immutable [[PiInstanceStore]] to use.
  * @param executionContext
  */
class MutexExecutor(var store: PiInstanceStore[Int] = SimpleInstanceStore[Int]())(
    implicit override val executionContext: ExecutionContext = ExecutionContext.global
) extends ProcessExecutor[Int]
    with SimplePiObservable[Int] {

  var ctr: Int = 0

  override protected def init(instance: PiInstance[_]): Future[Int] = this.synchronized {
    store = store.put(instance.copy(id = ctr))
    ctr = ctr + 1
    Future.successful(ctr - 1)
  }

  override def start(id: Int): Unit = this.synchronized {
    store.get(id) match {
      case None => publish(PiFailureNoSuchInstance(id))
      case Some(inst) => {
        publish(PiEventStart(inst))
        val ni = inst.reduce
        if (ni.completed) ni.result match {
          case None => {
            publish(PiFailureNoResult(ni))
            store = store.del(id)
          }
          case Some(res) => {
            publish(PiEventResult(ni, res))
            store = store.del(id)
          }
        }
        else {
          val (_, resi) = ni.handleThreads(handleThread(ni))
          store = store.put(resi)
        }
      }
    }
  }

  final def run(id: Int, f: PiInstance[Int] => PiInstance[Int]): Unit = this.synchronized {
    store.get(id) match {
      case None => publish(PiFailureNoSuchInstance(id))
      case Some(i) =>
        if (i.id != id)
          System.err.println("*** [" + id + "] Different instance ID encountered: " + i.id)
        else {
          val ni = f(i).reduce
          if (ni.completed) ni.result match {
            case None => {
              publish(PiFailureNoResult(ni))
              store = store.del(ni.id)
            }
            case Some(res) => {
              publish(PiEventResult(ni, res))
              store = store.del(ni.id)
            }
          }
          else {
            store = store.put(ni.handleThreads(handleThread(ni))._2)
          }
        }
    }
  }

  def handleThread(i: PiInstance[Int])(ref: Int, f: PiFuture): Boolean = {
    f match {
      case PiFuture(name, outChan, args) =>
        i.getProc(name) match {
          case None => {
            publish(PiFailureUnknownProcess(i, name))
            false
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
            true
          }
          case Some(p: CompositeProcess) => {
            publish(PiFailureAtomicProcessIsComposite(i, name))
            false
          } // TODO this should never happen!
        }
    }
  }

  def postResult(id: Int, ref: Int, res: PiObject): Unit = {
    run(id, { x => x.postResult(ref, res) })
  }
}
