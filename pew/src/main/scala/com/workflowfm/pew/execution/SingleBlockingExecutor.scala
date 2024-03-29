package com.workflowfm.pew.execution

import scala.annotation.tailrec
import scala.concurrent._
import scala.concurrent.duration.Duration

import com.workflowfm.pew._
import com.workflowfm.pew.stream.SimplePiObservable

/**
  * SingleBlockingExecutor fully executes one PiProcess from a map of given PiProcesses.
  * It blocks waiting for every atomic call to finish, so has no concurrency.
  */
case class SingleBlockingExecutor(implicit val context: ExecutionContext) { // extends ProcessExecutor[Int] with SimplePiObservable[Int] {
  def call(process: PiProcess, args: Seq[PiObject]): Option[Any] = {
    val s = process.execState(args)
    System.err.println(" === INITIAL STATE === \n" + s + "\n === === === === === === === ===")
    val fs = run(s)
    System.err.println(" === FINAL STATE === \n" + fs + "\n === === === === === === === ===")
    val res = fs.resources.sub(process.output._1)
    if (res.isGround) Some(PiObject.get(res))
    else None
  }

  @tailrec
  final def run(s: PiState): PiState = {
    val ns = s.fullReduce()
    if (ns.threads isEmpty) ns
    else run((ns /: ns.threads)(handleThread))
  }

  def handleThread(s: PiState, x: (Int, PiFuture)): PiState = x match {
    case (ref, f) =>
      handleThread(ref, f, s) getOrElse s
  }

  def handleThread(ref: Int, f: PiFuture, s: PiState): Option[PiState] = f match {
    case PiFuture(name, outChan, args) =>
      s.processes.get(name) match {
        case None => {
          System.err.println("*** ERROR *** Unable to find process: " + name)
          Some(s removeThread ref)
        }
        case Some(p: MetadataAtomicProcess) => {
          val f = p.runMeta(args map (_.obj))
          val res = Await.result(f, Duration.Inf)
          s.result(ref, res._1)
        }
        case Some(p: CompositeProcess) => {
          System.err.println("*** Executor encountered composite process thread: " + name); None
        } // TODO this should never happen!
      }
  }

  def execute(process: PiProcess, args: Seq[Any]): Option[Any] =
    call(process, args map PiObject.apply)
}
