package com.workflowfm.pew

case class PiInstance[T](final val id: T, called: Seq[Int], process: PiProcess, state: PiState) {
  def result: Option[Any] = {
    val res = state.resources.sub(process.output._1)
    if (res.isGround) {
      Some(PiObject.get(res))
    } else {
      None
    }
  }

  /**
   * Assuming a fullReduce, the workflow is completed iff:
   * (1) there are no new threads to be called
   * (2) there are no called threads to be returned
   */
  def completed: Boolean = state.threads.isEmpty && called.isEmpty

  /**
    * Post the result of a call, removing the completed call from the running calls, replace its output future with the
    *   value and fully reduce.
    *
    * @param thread Completed call thread
    * @param res Call result
    * @return Updated instance
    */
  def postResult(thread: Int, res: PiObject): PiInstance[T] = copy(
      called = called filterNot (_ == thread),
      state = state.result(thread, res)
        .map(_.fullReduce())
        .getOrElse(state)
    )

  /**
    * Post the results of multiple calls, removing the completed calls from the running calls, replacing their output
    *   futures with the values and fully reduce.
    *
    * @param allRes Sequence of thread-result pairs
    * @return Updated instance
    */
  def postResult( allRes: Seq[(Int, PiObject)] ): PiInstance[T] = copy(
      called = called filterNot (allRes.map(_._1) contains _),
      state = allRes.foldLeft( state )( (s, e) => s.result(e._1, e._2).getOrElse(s) )
        .fullReduce()
    )

  def reduce: PiInstance[T] = copy(state = state.fullReduce())

  def handleThreads(handler: (Int, PiFuture) => Boolean): (Seq[Int], PiInstance[T]) = {
    // Apply the thread handler to the state
    val newState = state.handleThreads(THandler(handler))

    // Retrieve the new set of threads
    val newCalls = newState.threads.keys.toSeq

    // Return the set of handled threads that were not yet called, as well as the updated instance
    (newCalls diff called, copy(called = newCalls, state = newState))
  }

  /**
   * Wrapper for thread handlers that ignores threads that have been already called.
   */
  private case class THandler(handler: (Int, PiFuture) => Boolean)
    extends (((Int, PiFuture)) => Boolean) {

    def apply(t: (Int, PiFuture)): Boolean =
      if (called contains t._1) {
        true
      } else {
        handler(t._1, t._2)
      }
  }

  def piFutureOf(ref: Int): Option[PiFuture] = state.threads.get(ref)

  def getProc(p: String): Option[PiProcess] = state.processes.get(p)

  def getAtomicProc(name: String): MetadataAtomicProcess = {
    getProc(name) match {
      case None => throw UnknownProcessException(this, name)
      case Some( proc ) => proc match {
        case atomic: MetadataAtomicProcess => atomic
        case _: PiProcess => throw AtomicProcessIsCompositeException(this, name)
      }
    }
  }

  /**
   * Is the workflow ready for simulation?
   */
  def simulationReady: Boolean =
    if (completed) {
      // Workflow is done
      true
    } else {
      // Workflow not done --> getProc failed or some call not converted to thread, or only simulated processes left

      // Get processes for all running threads
      val procs = state.threads flatMap { f => getProc(f._2.fun) }

      if (procs.isEmpty) {
        // getProc failed or some call not converted to thread
        false
      } else {
        // Are all open threads simulated processes?
        procs.forall(_.isSimulatedProcess)
      }
    }
}
object PiInstance {
  def apply[T](id: T, p: PiProcess, args: PiObject*): PiInstance[T] = PiInstance(id, Seq(), p, p.execState(args))

  /**
   * Construct a PiInstance as if we are making a call to ProcessExecutor.execute
   */
  def forCall[T](id: T, p: PiProcess, args: Any*): PiInstance[T]
    = PiInstance(id, Seq(), p, p execState (args map PiObject.apply))
}

trait PiInstanceStore[T] {
  def get(id: T): Option[PiInstance[T]]
  def put(i: PiInstance[T]): PiInstanceStore[T]
  def del(id: T): PiInstanceStore[T]
  def simulationReady: Boolean
}

trait PiInstanceMutableStore[T] {
  def get(id: T): Option[PiInstance[T]]
  def put(i: PiInstance[T]): Unit
  def del(id: T): Unit
  def simulationReady: Boolean
}

case class SimpleInstanceStore[T](m: Map[T, PiInstance[T]])
  extends PiInstanceStore[T] {

  override def get(id: T): Option[PiInstance[T]] = m.get(id)
  override def put(i: PiInstance[T]): SimpleInstanceStore[T] = copy(m = m + (i.id->i))
  override def del(id: T): SimpleInstanceStore[T] = copy(m = m - id)

  override def simulationReady: Boolean = m.values.forall(_.simulationReady)
  /*{
    m.values.foreach { i => {
      val procs = i.state.threads.values.map(_.fun).mkString(", ")
      println(s"**> [${i.id}] is waiting for procs: $procs")
    } }

  }*/
}

object SimpleInstanceStore {
  def apply[T](l: PiInstance[T]*): SimpleInstanceStore[T] =
    (SimpleInstanceStore( Map[T, PiInstance[T]]() ) /: l) (_.put(_))
}
