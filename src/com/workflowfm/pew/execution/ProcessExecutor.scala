package com.workflowfm.pew.execution

import com.workflowfm.pew._
import scala.concurrent._
import scala.concurrent.duration.Duration

/**
 * Executes an atomic process - blocking
 */
case class AtomicProcessExecutor(process:AtomicProcess) {
  def call(args:PiObject*)(implicit ec:ExecutionContext) = {
    val s = process.execState(args) fullReduce()
    s.threads.headOption match {
      case None => None
      case Some((ref,PiFuture(_, _, args))) => {
        val f = process.run(args map (_._1))
        val res = Await.result(f,Duration.Inf)
        s.result(ref,res) map (_.fullReduce()) map { x => PiObject.get(x.resources.sub(process.output._1)) }
      }
    }
  }
}

/**
 * Trait representing the ability to execute any PiProcess
 */
trait ProcessExecutor {
  implicit val context: ExecutionContext = ExecutionContext.global
	def execute(process:PiProcess,args:Seq[Any]):Future[Option[Any]]
}
object ProcessExecutor {
  def default = SingleBlockingExecutor(Map[String,PiProcess]())
  
  final case class AlreadyExecutingException(private val cause: Throwable = None.orNull)
                    extends Exception("Unable to execute more than one process at a time", cause) 
  final case class UnknownProcessException(val process:String, private val cause: Throwable = None.orNull)
                    extends Exception("Unknown process: " + process, cause) 
}

/**
 * Shortcut methods for unit testing
 */
trait ProcessExecutorTester {
  def exe(e:ProcessExecutor,p:PiProcess,args:Any*) = await(e.execute(p,args:Seq[Any]))
  def await[A](f:Future[A]):A = Await.result(f,Duration.Inf)  
}


