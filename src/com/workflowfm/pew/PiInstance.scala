package com.workflowfm.pew

case class PiInstance[T](final val id:T, called:Seq[Int], process:PiProcess, state:PiState) {
  def result:Option[Any] = {
    val res = state.resources.sub(process.output._1)
    if (res.isGround) Some(PiObject.get(res))
    else None
  }
  
  /**
   * Assuming a fullReduce, the workflow is completed if:
   * (1) there are no new threads to be called
   * (2) there are no called threads to be returned
   */
  def completed:Boolean = state.threads.isEmpty && called.isEmpty 
  
  def postResult(thread:Int, res:PiObject):PiInstance[T] = copy(
		  called = called filterNot (_ == thread),
		  state = state.result(thread,res).map(_.fullReduce()).getOrElse(state)
    )

  /** @return Post multiple thread results to this PiInstance simultaneously and fully reduce the result.
    */
  def postResult( allRes: Seq[(Int, PiObject)] ): PiInstance[T] = copy(
      called = called filterNot (allRes.map(_._1) contains _),
      state = allRes.foldLeft( state )( (s,e) => s.result(e._1, e._2).getOrElse(s) ).fullReduce()
    )

  def reduce:PiInstance[T] = copy(state = state.fullReduce())
  
  def handleThreads(handler:(Int,PiFuture)=>Boolean):(Seq[Int],PiInstance[T]) = {
    val newstate = state.handleThreads(THandler(handler))
    val newcalls = newstate.threads.keys.toSeq
    (newcalls diff called,copy(called=newcalls,state=newstate))
  }

  /**
   * Wrapper for thread handlers that ignores threads that have been already called.
   */
  private case class THandler(handler:(Int,PiFuture)=>Boolean) extends (((Int,PiFuture)) => Boolean) {
    def apply(t:(Int,PiFuture)):Boolean = 
      if (called contains t._1) true
      else handler(t._1,t._2)
  }
  
  def piFutureOf(ref:Int):Option[PiFuture] = state.threads.get(ref)
  
  def getProc(p:String):Option[PiProcess] = state.processes.get(p)
  
  /**
   * Should the simulator wait for the workflow?
   */
  def simulationReady:Boolean = 
    if (completed) true // workflow is done
    else {
      val procs = state.threads flatMap { f => getProc(f._2.fun) }
      if (procs.isEmpty) false // workflow is not completed, so we either couldn't find a process with getProc or 
                               // calls have not been converted to threads yet (so no fullreduce) for whatever reason
      else procs.forall(_.isSimulatedProcess) // are all open threads simulated processes?
    }
}
object PiInstance {
  def apply[T](id:T,p:PiProcess,args:PiObject*):PiInstance[T] = PiInstance(id, Seq(), p, p.execState(args))
}




trait PiInstanceStore[T] {
  def get(id:T):Option[PiInstance[T]]
  def put(i:PiInstance[T]):PiInstanceStore[T]
  def del(id:T):PiInstanceStore[T]
  def simulationReady:Boolean
}

trait PiInstanceMutableStore[T] {
  def get(id:T):Option[PiInstance[T]]
  def put(i:PiInstance[T]):Unit
  def del(id:T):Unit
  def simulationReady:Boolean
}

case class SimpleInstanceStore(m:Map[Int,PiInstance[Int]]) extends PiInstanceStore[Int] {
  def get(id:Int) = m.get(id)
  def put(i:PiInstance[Int]) = copy(m = m + (i.id->i))
  def del(id:Int) = copy(m = m - id)
  def simulationReady:Boolean = m.values.forall(_.simulationReady)
}
object SimpleInstanceStore {
  def apply(l:PiInstance[Int]*):SimpleInstanceStore = (SimpleInstanceStore(Map[Int,PiInstance[Int]]()) /: l) (_.put(_))
}
