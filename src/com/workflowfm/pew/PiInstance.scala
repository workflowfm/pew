package com.workflowfm.pew

case class PiInstance(final val id:Int, called:Seq[Int], process:PiProcess, state:PiState) {
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
  
  def postResult(thread:Int, res:PiObject):PiInstance = copy(
		  called = called filterNot (_ == thread),
		  state = state.result(thread,res).map(_.fullReduce()).getOrElse(state)
    )

  def reduce:PiInstance = copy(state = state.fullReduce())
  
  def handleThreads(handler:(Int,PiFuture)=>Boolean):PiInstance = {
    val newstate = state.handleThreads(THandler(handler))
    copy(called=newstate.threads.keys.toSeq,state=newstate)
  }

  /**
   * Wrapper for thread handlers that ignores threads that have been already called.
   */
  private case class THandler(handler:(Int,PiFuture)=>Boolean) extends (((Int,PiFuture)) => Boolean) {
    def apply(t:(Int,PiFuture)):Boolean = 
      if (called contains t._1) true
      else if (handler(t._1,t._2)) true
      else false 
  }
}
object PiInstance {
  def apply(id:Int,p:PiProcess,args:PiObject*):PiInstance = PiInstance(id, Seq(), p, p.execState(args))
}




trait PiInstanceStore {
  def get(id:Int):Option[PiInstance]
  def put(i:PiInstance):PiInstanceStore
  def del(id:Int):PiInstanceStore
}

case class SimpleInstanceStore(m:Map[Int,PiInstance]) extends PiInstanceStore {
  def get(id:Int) = m.get(id)
  def put(i:PiInstance) = copy(m = m + (i.id->i))
  def del(id:Int) = copy(m = m - id)
}
object SimpleInstanceStore {
  def apply(l:PiInstance*):SimpleInstanceStore = (SimpleInstanceStore(Map[Int,PiInstance]()) /: l) (_.put(_))
}